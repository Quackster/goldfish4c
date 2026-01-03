// gcc -O2 -Wall -Wextra -o v1_client v1_client.c
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DELIM "\r##"

static int connect_tcp(const char *host, int port) {
    struct addrinfo hints, *res, *rp;
    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    int err = getaddrinfo(host, portstr, &hints, &res);
    if (err != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(err));
        return -1;
    }

    int sock = -1;
    for (rp = res; rp; rp = rp->ai_next) {
        sock = (int)socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock < 0) continue;
        if (connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(sock);
        sock = -1;
    }
    freeaddrinfo(res);
    return sock;
}

static int send_packet(int sock, const char *payload, size_t len) {
    if (len > 9999) return -1;
    char hdr[5];
    // Right-aligned 4 chars (spaces ok for server parser)
    snprintf(hdr, sizeof(hdr), "%4d", (int)len);

    if (send(sock, hdr, 4, 0) != 4) return -1;
    if (len > 0 && send(sock, payload, len, 0) != (ssize_t)len) return -1;
    return 0;
}

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    int port = 37120;
    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = atoi(argv[2]);

    int sock = connect_tcp(host, port);
    if (sock < 0) {
        fprintf(stderr, "connect failed\n");
        return 1;
    }
    fprintf(stderr, "[connected to %s:%d]\n", host, port);

    // simple non-threaded: use select() to read server while you type
    fd_set rfds;
    char in[4096];

    char buf[65536];
    size_t buflen = 0;

    while (1) {
        FD_ZERO(&rfds);
        FD_SET(sock, &rfds);
        FD_SET(STDIN_FILENO, &rfds);
        int maxfd = sock > STDIN_FILENO ? sock : STDIN_FILENO;

        if (select(maxfd + 1, &rfds, NULL, NULL, NULL) < 0) {
            perror("select");
            break;
        }

        if (FD_ISSET(sock, &rfds)) {
            char tmp[4096];
            ssize_t r = recv(sock, tmp, sizeof(tmp), 0);
            if (r <= 0) {
                fprintf(stderr, "\n[server closed]\n");
                break;
            }
            if (buflen + (size_t)r > sizeof(buf)) buflen = 0; // crude reset
            memcpy(buf + buflen, tmp, (size_t)r);
            buflen += (size_t)r;

            // split on "\r##"
            for (;;) {
                char *p = NULL;
                for (size_t i = 0; i + 2 < buflen; i++) {
                    if (buf[i] == '\r' && buf[i+1] == '#' && buf[i+2] == '#') {
                        p = buf + i;
                        break;
                    }
                }
                if (!p) break;
                size_t framelen = (size_t)(p - buf);

                // strip optional "# " prefix
                size_t start = 0;
                if (framelen >= 2 && buf[0] == '#' && buf[1] == ' ') start = 2;

                fwrite("<-- ", 1, 4, stdout);
                fwrite(buf + start, 1, framelen - start, stdout);
                fwrite("\n", 1, 1, stdout);

                // remove frame + delim
                size_t remain = buflen - (framelen + 3);
                memmove(buf, p + 3, remain);
                buflen = remain;
            }
        }

        if (FD_ISSET(STDIN_FILENO, &rfds)) {
            if (!fgets(in, sizeof(in), stdin)) break;
            // strip \n
            size_t n = strlen(in);
            if (n && in[n-1] == '\n') in[n-1] = '\0';

            if (strcmp(in, "/quit") == 0 || strcmp(in, "/exit") == 0) break;

            if (send_packet(sock, in, strlen(in)) != 0) {
                fprintf(stderr, "send_packet failed\n");
                break;
            }
        }
    }

    close(sock);
    return 0;
}

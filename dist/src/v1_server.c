#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include <windows.h>
    #include <process.h>
    #pragma comment(lib, "ws2_32.lib")

    // Windows thread compatibility
    typedef HANDLE pthread_t;
    typedef CRITICAL_SECTION pthread_mutex_t;
    #define PTHREAD_MUTEX_INITIALIZER {0}

    // Thread function wrapper for Windows
    typedef struct {
        void *(*start_routine)(void *);
        void *arg;
    } thread_wrapper_data;

    DWORD WINAPI thread_wrapper(LPVOID lpParam) {
        thread_wrapper_data *data = (thread_wrapper_data *)lpParam;
        void *(*start_routine)(void *) = data->start_routine;
        void *arg = data->arg;
        free(data);
        start_routine(arg);
        return 0;
    }

    int pthread_create(pthread_t *thread, void *attr, void *(*start_routine)(void *), void *arg) {
        (void)attr;
        thread_wrapper_data *data = (thread_wrapper_data*)malloc(sizeof(thread_wrapper_data));
        if (!data) return -1;
        data->start_routine = start_routine;
        data->arg = arg;
        *thread = CreateThread(NULL, 0, thread_wrapper, data, 0, NULL);
        return (*thread == NULL) ? -1 : 0;
    }

    void pthread_mutex_lock(pthread_mutex_t *mutex) {
        EnterCriticalSection(mutex);
    }

    void pthread_mutex_unlock(pthread_mutex_t *mutex) {
        LeaveCriticalSection(mutex);
    }

    int pthread_mutex_init(pthread_mutex_t *mutex, void *attr) {
        (void)attr;
        InitializeCriticalSection(mutex);
        return 0;
    }

    int pthread_mutex_destroy(pthread_mutex_t *mutex) {
        DeleteCriticalSection(mutex);
        return 0;
    }

    void pthread_detach(pthread_t thread) {
        CloseHandle(thread);
    }

    int pthread_cancel(pthread_t thread) {
        TerminateThread(thread, 0);
        return 0;
    }

    int pthread_join(pthread_t thread, void **retval) {
        (void)retval;
        WaitForSingleObject(thread, INFINITE);
        CloseHandle(thread);
        return 0;
    }

    typedef int socklen_t;
    #define close closesocket
    #define sleep(x) Sleep((x)*1000)
    #define usleep(x) Sleep((x)/1000)
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    #include <pthread.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mysql/mysql.h>

#define MAX_CLIENTS 150
#define BUFFER_SIZE 4096
#define PACKET_HEADER_SIZE 4
#define INVALID_SOCKET -1

typedef struct {
    int id;
    int socket;
    char name[50];
    char password[50];
    int credits;
    char email[100];
    char figure[100];
    char birthday[20];
    char phonenumber[20];
    char customData[256];
    int had_read_agreement;
    char sex[10];
    char country[50];
    char has_special_rights[10];
    char badge_type[20];
    int inroom;
    int owner;
    int rights;
    int teleport;
    int moonwalk;
    int dance;
    int userx;
    int usery;
    int userz;
    int userrx;
    int userry;
    char roomcache[2048];
    int handcache;
    int used;
    int server;
    pthread_t pathfinding_thread;
    int pathfinding_active;
    int target_x;
    int target_y;
    MYSQL* db_conn;
} User;

User users[MAX_CLIENTS];
pthread_mutex_t users_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t db_mutex = PTHREAD_MUTEX_INITIALIZER;
MYSQL* global_db_conn = NULL;

// Function prototypes
#include <ctype.h>  // add this include at top for isdigit

static int recv_all(int sock, void *buf, int len) {
    char *p = (char*)buf;
    int got = 0;
    while (got < len) {
        int r = recv(sock, p + got, len - got, 0);
        if (r <= 0) return r; // 0 = disconnect, <0 = error
        got += r;
    }
    return got;
}

// Parse 4-byte ASCII length header into 0..9999
static int parse_len4(const char hdr[4]) {
    char tmp[5];
    memcpy(tmp, hdr, 4);
    tmp[4] = '\0';

    // Accept digits and spaces only (so "  12" works).
    // Build a digits-only string; reject anything else.
    char digits[5];
    int j = 0;
    for (int i = 0; i < 4; i++) {
        unsigned char c = (unsigned char)tmp[i];
        if (c == ' ') continue;
        if (!isdigit(c)) return -1;
        if (j < 4) digits[j++] = (char)c;
    }
    digits[j] = '\0';

    // Empty (all spaces) => 0
    if (j == 0) return 0;

    long v = strtol(digits, NULL, 10);
    if (v < 0 || v > 9999) return -1;
    return (int)v;
}
void* handle_client(void* arg);
void process_packet(int client_id, char* packet);
void send_data(int client_id, const char* data);
void send_data_all(const char* data);
void send_data_room(int room_id, const char* data);
void load_user_data(int client_id, const char* username, const char* password);
void load_wordfilters();
char* filter_chat_message(char* message);
void* pathfinding_thread(void* arg);
int get_room_height(int x, int y, char* heightmap);
void init_database();
void cleanup_user(int client_id);
char* escape_string(MYSQL* conn, const char* str);

static int send_cmd(User *c, const char *data) {
    if (!c || c->socket == INVALID_SOCKET) return -1;
    if (!data) data = "";

    size_t dlen = strlen(data);
    size_t plen = 2 + dlen + 3; /* "# " + data + "\r##" */
    if (plen > 9999) return -1;

    char *payload = (char*)malloc(plen + 1);
    if (!payload) return -1;

    memcpy(payload, "# ", 2);
    memcpy(payload + 2, data, dlen);
    memcpy(payload + 2 + dlen, "\r##", 3);
    payload[plen] = '\0';

    int rc = send(c->socket, payload, (int)plen, 0);
    printf("<-- %s\n", payload);

    free(payload);
    return rc;
}

int main() {
#ifdef _WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        printf("WSAStartup failed\n");
        return 1;
    }
#endif
    int server_fd, new_socket;
    struct sockaddr_in address;
    int opt = 1;
    int addrlen = sizeof(address);
    pthread_t thread_id;

    // IMPORTANT (fixes Windows crash/segfault on first lock):
    pthread_mutex_init(&users_mutex, NULL);
    pthread_mutex_init(&db_mutex, NULL);

    // Initialize users
    for (int i = 0; i < MAX_CLIENTS; i++) {
        memset(&users[i], 0, sizeof(User));
        users[i].used = 0;
        users[i].id = i;
        users[i].socket = INVALID_SOCKET;
        users[i].db_conn = NULL;
        users[i].pathfinding_thread = (pthread_t)0;
    }

    // Initialize database connection
    init_database();

    // Create socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Set socket options
#ifdef _WIN32
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt))) {
#else
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
#endif
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(37120);

    // Bind socket
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for connections
    if (listen(server_fd, 10) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("Server listening on port 37120\n");

    // Accept connections
    while (1) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept");
            continue;
        }

        // Find available user slot
        int client_id = -1;
        pthread_mutex_lock(&users_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!users[i].used) {
                client_id = i;
                break;
            }
        }
        pthread_mutex_unlock(&users_mutex);

        if (client_id == -1) {
            printf("Server full, rejecting connection\n");
            close(new_socket);
            continue;
        }

        // Initialize user (fixes uninitialized fields / thread handle)
        pthread_mutex_lock(&users_mutex);
        memset(&users[client_id], 0, sizeof(User));
        users[client_id].id = client_id;
        users[client_id].socket = new_socket;
        users[client_id].used = 1;
        users[client_id].inroom = 0;
        users[client_id].owner = 0;
        users[client_id].rights = 0;
        users[client_id].teleport = 0;
        users[client_id].moonwalk = 0;
        users[client_id].dance = 0;
        users[client_id].userx = 0;
        users[client_id].usery = 0;
        users[client_id].userz = 0;
        users[client_id].userrx = 0;
        users[client_id].userry = 0;
        users[client_id].handcache = 0;
        users[client_id].server = 0;
        users[client_id].pathfinding_active = 0;
        users[client_id].pathfinding_thread = (pthread_t)0;
        users[client_id].name[0] = '\0';
        users[client_id].password[0] = '\0';
        users[client_id].db_conn = NULL;
        pthread_mutex_unlock(&users_mutex);

        // Create database connection for this user
        users[client_id].db_conn = mysql_init(NULL);
        if (!users[client_id].db_conn ||
            !mysql_real_connect(users[client_id].db_conn, "localhost", "root", "verysecret", "goldfish4c", 0, NULL, 0)) {
            printf("Failed to connect to database for client %d\n", client_id);
            close(new_socket);
            pthread_mutex_lock(&users_mutex);
            users[client_id].used = 0;
            users[client_id].socket = INVALID_SOCKET;
            if (users[client_id].db_conn) {
                mysql_close(users[client_id].db_conn);
                users[client_id].db_conn = NULL;
            }
            pthread_mutex_unlock(&users_mutex);
            continue;
        }

        // Send HELLO packet (now wrapped in # <data>\r##)
        send_data(client_id, "HELLO\r");

        // Create thread to handle client (FIX: heap-allocated arg)
        int *cid = (int*)malloc(sizeof(int));
        if (!cid) {
            printf("malloc failed for client thread arg\n");
            cleanup_user(client_id);
            continue;
        }
        *cid = client_id;

        if (pthread_create(&thread_id, NULL, handle_client, cid) < 0) {
            perror("could not create thread");
            free(cid);
            cleanup_user(client_id);
            continue;
        }

        pthread_detach(thread_id);
        printf("Client connected: %d\n", client_id);
    }

    if (global_db_conn) {
        mysql_close(global_db_conn);
    }

#ifdef _WIN32
    WSACleanup();
#endif
    return 0;
}

void init_database() {
    global_db_conn = mysql_init(NULL);
    if (!mysql_real_connect(global_db_conn, "localhost", "root", "verysecret", "goldfish4c", 0, NULL, 0)) {
        printf("Failed to initialize global database connection\n");
        exit(EXIT_FAILURE);
    }
}

void* handle_client(void* arg) {
    int client_id = *(int*)arg;
    free(arg);

    int sock = users[client_id].socket;

    while (1) {
        char hdr[4];

        // Read exactly 4 bytes header (ASCII length)
        int hr = recv_all(sock, hdr, 4);
        if (hr <= 0) break;

        int len_info = parse_len4(hdr);
        if (len_info < 0) {
            // Bad header -> disconnect or ignore
            printf("Bad length header: [%c%c%c%c]\n", hdr[0], hdr[1], hdr[2], hdr[3]);
            break;
        }

        // If length is 0, it's a valid empty frame
        if (len_info == 0) {
            process_packet(client_id, (char*)"");
            continue;
        }

        // Protocol max is 9999; allocate so you aren't limited by BUFFER_SIZE
        char *packet = (char*)malloc((size_t)len_info + 1);
        if (!packet) break;

        int pr = recv_all(sock, packet, len_info);
        if (pr <= 0) {
            free(packet);
            break;
        }

        packet[len_info] = '\0';
        printf("--> %s\n", packet);

        process_packet(client_id, packet);
        free(packet);

        usleep(20000);
    }

    printf("Client disconnected: %d\n", client_id);
    cleanup_user(client_id);
    return NULL;
}

void cleanup_user(int client_id) {
    pthread_mutex_lock(&users_mutex);

    if (client_id < 0 || client_id >= MAX_CLIENTS) {
        pthread_mutex_unlock(&users_mutex);
        return;
    }

    if (users[client_id].used) {
        if (users[client_id].inroom != 0) {
            char query[256];
            sprintf(query, "UPDATE rooms SET inroom = inroom - 1 WHERE id = %d", users[client_id].inroom);
            mysql_query(users[client_id].db_conn, query);
            users[client_id].inroom = 0;
            users[client_id].owner = 0;
        }

        if (users[client_id].pathfinding_active) {
            users[client_id].pathfinding_active = 0;
            // Only join if we had a thread handle
            if (users[client_id].pathfinding_thread) {
                pthread_join(users[client_id].pathfinding_thread, NULL);
            }
            users[client_id].pathfinding_thread = (pthread_t)0;
        }

        if (users[client_id].socket > 0) {
            close(users[client_id].socket);
        }
        users[client_id].socket = INVALID_SOCKET;

        if (users[client_id].db_conn) {
            mysql_close(users[client_id].db_conn);
            users[client_id].db_conn = NULL;
        }

        users[client_id].used = 0;
    }

    pthread_mutex_unlock(&users_mutex);
}

char* escape_string(MYSQL* conn, const char* str) {
    static char escaped[1024];
    if (!conn || !str) {
        escaped[0] = '\0';
        return escaped;
    }
    mysql_real_escape_string(conn, escaped, str, (unsigned long)strlen(str));
    return escaped;
}

void process_packet(int client_id, char* packet) {
    char packet_copy[BUFFER_SIZE];
    strcpy(packet_copy, packet);
    
    char* token = strtok(packet, " ");
    if (!token) return;
    
    if (strcmp(token, "VERSIONCHECK") == 0) {
        token = strtok(NULL, " ");
        if (token && (strcmp(token, "client002") == 0 || strcmp(token, "client003") == 0)) {
            send_data(client_id, "ENCRYPTION_OFF");
            send_data(client_id, "SECRET_KEY\r1337");
        }
    }
    else if (strcmp(token, "LOGIN") == 0) {
        char* username = strtok(NULL, " ");
        char* password = strtok(NULL, " ");
		
		printf("Username: %s\n", username);
		printf("Password: %s\n", password);
        
        if (username && password) {
            char query[512];
            sprintf(query, "SELECT * FROM users WHERE name = '%s' AND password = '%s'", 
                    escape_string(users[client_id].db_conn, username),
                    escape_string(users[client_id].db_conn, password));
					
			printf("Query: %s\n", query);
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                if (mysql_num_rows(result) > 0) {
                    load_user_data(client_id, username, password);
                    
                    // Check if server parameter exists
                    char* server_param = strtok(NULL, " ");
                    if (server_param && strcmp(server_param, "0") == 0) {
                        users[client_id].server = 1;
                        
                        // Send lobby objects
                        send_data(client_id, "OBJECTS WORLD 0 lobby_a\r"
                            "f90 flower1 9 0 7 0\r"
                            "S110 chairf2b 11 0 7 4\r"
                            "s120 chairf2 12 0 7 4\r"
                            "t130 table1 13 0 7 2\r"
                            "S140 chairf2b 14 0 7 4\r"
                            "s150 chairf2 15 0 7 4\r"
                            "w160 watermatic 16 0 7 4\r"
                            "T92 telkka 9 2 7 2\r"
                            "f93 flower1 9 3 7 0\r"
                            "Z113 chairf2d 11 3 7 0\r"
                            "s123 chairf2 12 3 7 0\r"
                            "t133 table1 13 3 7 2\r"
                            "Z143 chairf2d 14 3 7 0\r"
                            "s153 chairf2 15 3 7 0\r"
                            "f124 flower1 12 4 3 0\r"
                            "f164 flower1 16 4 3 0\r"
                            "S07 chairf2b 0 7 3 4\r"
                            "s17 chairf2 1 7 3 4\r"
                            "Z010 chairf2d 0 10 3 0\r"
                            "s110 chairf2 1 10 3 0\r"
                            "r2112 roommatic 21 12 1 4\r"
                            "r2212 roommatic 22 12 1 4\r"
                            "r2312 roommatic 23 12 1 4\r"
                            "r2412 roommatic 24 12 1 4\r"
                            "S014 chairf2b 0 14 3 4\r"
                            "s114 chairf2 1 14 3 4\r"
                            "w1314 watermatic 13 14 1 2\r"
                            "w1215 watermatic 12 15 1 4\r"
                            "c1916 chairf1 19 16 1 4\r"
                            "C2116 table2c 21 16 1 2\r"
                            "c2316 chairf1 23 16 1 4\r"
                            "Z017 chairf2d 0 17 3 0\r"
                            "s117 chairf2 1 17 3 0\r"
                            "D2117 table2b 21 17 1 2\r"
                            "c1918 chairf1 19 18 1 0\r"
                            "d2118 table2 21 18 1 2\r"
                            "c2318 chairf1 23 18 1 0\r"
                            "S721 chairf2b 7 21 2 2\r"
                            "z722 chairf2c 7 22 2 2\r"
                            "z723 chairf2c 7 23 2 2\r"
                            "z724 chairf2c 7 24 2 2\r"
                            "s725 chairf2 7 25 2 2\r"
                            "t726 table1 7 26 2 2\r"
                            "e1026 flower2 10 26 1 2");
                        
                        // Send heightmap
                        strcpy(users[client_id].roomcache, "xxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx");
                        send_data(client_id, "HEIGHTMAPxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx");
                        
                        // Send user info
                        char user_packet[512];
                        sprintf(user_packet, "USERS\r %s %s 3 5 0 %s", 
                            users[client_id].name, 
                            users[client_id].figure + 7, // Remove "figure=" prefix
                            users[client_id].customData);
                        send_data(client_id, user_packet);
                        
                        users[client_id].userx = 12;
                        users[client_id].usery = 27;
                        
                        char status_packet[256];
                        sprintf(status_packet, "STATUS \r%s 12,27,1,0,0/mod 0/", users[client_id].name);
                        send_data(client_id, status_packet);
                    }
                } else {
                    send_data(client_id, "ERROR: login incorrect");
                }
                mysql_free_result(result);
            } else {
                send_data(client_id, "ERROR: login incorrect");
            }
        }
    }
    else if (strcmp(token, "INFORETRIEVE") == 0) {
        char* username = strtok(NULL, " ");
        char* password = strtok(NULL, " ");
        
        if (username && password) {
            load_user_data(client_id, username, password);
            
            char user_object[2048];
            sprintf(user_object, "USEROBJECT\r"
                    "name=%s\r"
                    "email=%s\r"
                    "figure=%s\r"
                    "birthday=%s\r"
                    "phonenumber=%s\r"
                    "customData=%s\r"
                    "had_read_agreement=%d\r"
                    "sex=%s\r"
                    "country=%s\r"
                    "has_special_rights=%s\r"
                    "badge_type=%s\r",
                    users[client_id].name,
                    users[client_id].email,
                    users[client_id].figure,
                    users[client_id].birthday,
                    users[client_id].phonenumber,
                    users[client_id].customData,
                    users[client_id].had_read_agreement,
                    users[client_id].sex,
                    users[client_id].country,
                    users[client_id].has_special_rights,
                    users[client_id].badge_type);
            send_data(client_id, user_object);
        }
    }
    else if (strcmp(token, "INITUNITLISTENER") == 0) {
        if (mysql_query(users[client_id].db_conn, "SELECT * FROM pubs") == 0) {
            MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
            MYSQL_ROW row;
            char room_data[4096] = "";
            
            while ((row = mysql_fetch_row(result))) {
                char temp[512];
                sprintf(temp, "%s,%s,%s/83.117.80.215/83.117.80.215,37120,%s\t%s,%s,%s,%s\r",
                        row[1], row[2], row[3], row[1], row[4], row[2], row[3], row[5]);
                strcat(room_data, temp);
            }
            
            char all_units[4096];
            sprintf(all_units, "ALLUNITS\r%s", room_data);
            send_data(client_id, all_units);
            mysql_free_result(result);
        }
    }
    else if (strcmp(token, "SEARCHBUSYFLATS") == 0) {
        if (mysql_query(users[client_id].db_conn, "SELECT * FROM rooms ORDER BY inroom DESC") == 0) {
            MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
            MYSQL_ROW row;
            char room_data[4096] = "";
            
            while ((row = mysql_fetch_row(result))) {
                char temp[512];
                sprintf(temp, "\r%s/%s/%s/%s/%s/%s/83.117.80.215/83.117.80.215/37120/%s/null/%s",
                        row[0], row[1], row[3], row[4], row[5], row[6], row[10], row[12]);
                strcat(room_data, temp);
            }
            
            char busy_flats[4096];
            sprintf(busy_flats, "BUSY_FLAT_RESULTS 1%s", room_data);
            send_data(client_id, busy_flats);
            mysql_free_result(result);
        }
    }
    else if (strcmp(token, "GETCREDITS") == 0) {
        char wallet[128];
        sprintf(wallet, "WALLETBALANCE\r%d", users[client_id].credits);
        send_data(client_id, wallet);
        send_data(client_id, "MESSENGERSMSACCOUNT\rnoaccount");
        send_data(client_id, "MESSENGERREADY \r");
    }
    else if (strcmp(token, "REGISTER") == 0) {
        // Parse registration data
        char* lines[15];
        int line_count = 0;
        char* line = strtok(packet_copy, "\r");
        while (line && line_count < 15) {
            lines[line_count++] = line;
            line = strtok(NULL, "\r");
        }
        
        if (line_count >= 11) {
            char username[50], password[50], email[100], figure[100];
            char birthday[20], phone[20], mission[256], agree[10], sex[10], city[50];
            
            sscanf(lines[0], "REGISTER name=%s", username);
            sscanf(lines[1], "password=%s", password);
            sscanf(lines[2], "email=%s", email);
            sscanf(lines[3], "figure=%s", figure);
            sscanf(lines[5], "birthday=%s", birthday);
            sscanf(lines[6], "phonenumber=%s", phone);
            sscanf(lines[7], "customData=%[^\r]", mission);
            sscanf(lines[8], "had_read_agreement=%s", agree);
            sscanf(lines[9], "sex=%s", sex);
            sscanf(lines[10], "country=%s", city);
            
            char query[1024];
            sprintf(query, "INSERT INTO users (name, password, credits, email, figure, birthday, phonenumber, customData, had_read_agreement, sex, country, has_special_rights, badge_type) VALUES ('%s', '%s', '1337', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '0', '0')",
                    escape_string(users[client_id].db_conn, username),
                    escape_string(users[client_id].db_conn, password),
                    escape_string(users[client_id].db_conn, email),
                    escape_string(users[client_id].db_conn, figure),
                    escape_string(users[client_id].db_conn, birthday),
                    escape_string(users[client_id].db_conn, phone),
                    escape_string(users[client_id].db_conn, mission),
                    escape_string(users[client_id].db_conn, agree),
                    escape_string(users[client_id].db_conn, sex),
                    escape_string(users[client_id].db_conn, city));
            
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "UPDATE") == 0) {
        // Parse update data
        char* lines[15];
        int line_count = 0;
        char* line = strtok(packet_copy, "\r");
        while (line && line_count < 15) {
            lines[line_count++] = line;
            line = strtok(NULL, "\r");
        }
        
        if (line_count >= 11) {
            char username[50], password[50], email[100], figure[100];
            char birthday[20], phone[20], mission[256], agree[10], sex[10], city[50];
            
            sscanf(lines[0], "UPDATE name=%s", username);
            sscanf(lines[1], "password=%s", password);
            sscanf(lines[2], "email=%s", email);
            sscanf(lines[3], "figure=%s", figure);
            sscanf(lines[5], "birthday=%s", birthday);
            sscanf(lines[6], "phonenumber=%s", phone);
            sscanf(lines[7], "customData=%[^\r]", mission);
            sscanf(lines[8], "had_read_agreement=%s", agree);
            sscanf(lines[9], "sex=%s", sex);
            sscanf(lines[10], "country=%s", city);
            
            char query[1024];
            sprintf(query, "UPDATE users SET password = '%s', email = '%s', figure = '%s', birthday = '%s', phonenumber = '%s', had_read_agreement = '%s', country = '%s', sex = '%s', customData = '%s' WHERE name = '%s' LIMIT 1",
                    escape_string(users[client_id].db_conn, password),
                    escape_string(users[client_id].db_conn, email),
                    escape_string(users[client_id].db_conn, figure),
                    escape_string(users[client_id].db_conn, birthday),
                    escape_string(users[client_id].db_conn, phone),
                    escape_string(users[client_id].db_conn, agree),
                    escape_string(users[client_id].db_conn, city),
                    escape_string(users[client_id].db_conn, sex),
                    escape_string(users[client_id].db_conn, mission),
                    escape_string(users[client_id].db_conn, username));
            
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "SEARCHFLATFORUSER") == 0) {
        char* user = strchr(packet_copy, '/') + 1;
        if (user) {
            char* newline = strchr(user, '\r');
            if (newline) *newline = '\0';
            
            char query[256];
            sprintf(query, "SELECT * FROM rooms WHERE owner = '%s'", escape_string(users[client_id].db_conn, user));
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                MYSQL_ROW row;
                char room_data[2048] = "";
                
                while ((row = mysql_fetch_row(result))) {
                    char temp[256];
                    sprintf(temp, "\r%s/%s/%s/%s/%s/%s/83.117.80.215/83.117.80.215/37120/0/null/%s",
                            row[0], row[1], row[3], row[4], row[5], row[6], row[12]);
                    strcat(room_data, temp);
                }
                
                char busy_flats[2048];
                sprintf(busy_flats, "BUSY_FLAT_RESULTS 1%s", room_data);
                send_data(client_id, busy_flats);
                mysql_free_result(result);
            }
        }
    }
    else if (strcmp(token, "UINFO_MATCH") == 0) {
        char* user = strchr(packet_copy, '/') + 1;
        if (user) {
            char* newline = strchr(user, '\r');
            if (newline) *newline = '\0';
            
            char query[256];
            sprintf(query, "SELECT * FROM users WHERE name = '%s'", escape_string(users[client_id].db_conn, user));
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                if (mysql_num_rows(result) > 0) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    char member_info[512];
                    sprintf(member_info, "MEMBERINFO MESSENGER\r%s\r%s\r\r0\r%s%s",
                            user, row[8], row[4] + 7, row[10]);
                    send_data(client_id, member_info);
                } else {
                    char no_such_user[128];
                    sprintf(no_such_user, "NOSUCHUSER MESSENGER\r%s", user);
                    send_data(client_id, no_such_user);
                }
                mysql_free_result(result);
            }
        }
    }
    else if (strcmp(token, "GETFLATINFO") == 0) {
        send_data(client_id, "SETFLATINFO\r/1/");
    }
    else if (strcmp(token, "DELETEFLAT") == 0) {
        char* flat_id = strchr(packet_copy, '/') + 1;
        if (flat_id) {
            char* newline = strchr(flat_id, '\r');
            if (newline) *newline = '\0';
            
            char query[128];
            sprintf(query, "DELETE FROM rooms WHERE id = '%s'", flat_id);
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "GET_FAVORITE_ROOMS") == 0) {
        char* username = strtok(NULL, " ");
        if (username) {
            char query[256];
            sprintf(query, "SELECT * FROM favrooms WHERE user = '%s'", escape_string(users[client_id].db_conn, username));
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                MYSQL_ROW row;
                char room_data[2048] = "";
                
                while ((row = mysql_fetch_row(result))) {
                    char inner_query[128];
                    sprintf(inner_query, "SELECT * FROM rooms WHERE id = %s LIMIT 1", row[2]);
                    
                    if (mysql_query(users[client_id].db_conn, inner_query) == 0) {
                        MYSQL_RES* inner_result = mysql_store_result(users[client_id].db_conn);
                        if (mysql_num_rows(inner_result) > 0) {
                            MYSQL_ROW inner_row = mysql_fetch_row(inner_result);
                            char temp[256];
                            sprintf(temp, "\r%s/%s/%s/%s/%s/%s/83.117.80.215/83.117.80.215/37120/%s/null/%s",
                                    inner_row[0], inner_row[1], inner_row[3], inner_row[4], 
                                    inner_row[5], inner_row[6], inner_row[10], inner_row[12]);
                            strcat(room_data, temp);
                        }
                        mysql_free_result(inner_result);
                    }
                }
                
                char busy_flats[2048];
                sprintf(busy_flats, "BUSY_FLAT_RESULTS 1%s", room_data);
                send_data(client_id, busy_flats);
                mysql_free_result(result);
            }
        }
    }
    else if (strcmp(token, "ADD_FAVORITE_ROOM") == 0) {
        char* room_id = strtok(NULL, " ");
        if (room_id) {
            char query[256];
            sprintf(query, "INSERT INTO favrooms VALUES (NULL, '%s', '%s')",
                    escape_string(users[client_id].db_conn, users[client_id].name),
                    room_id);
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "DEL_FAVORITE_ROOM") == 0) {
        char* room_id = strtok(NULL, " ");
        if (room_id) {
            char query[128];
            sprintf(query, "DELETE FROM favrooms WHERE roomid = '%s' LIMIT 1", room_id);
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "SEARCHFLAT") == 0) {
        char* search_term = strchr(packet_copy, '/') + 1;
        if (search_term) {
            char* newline = strchr(search_term, '\r');
            if (newline) *newline = '\0';
            
            char query[256];
            sprintf(query, "SELECT * FROM rooms WHERE owner LIKE '%%%s%%' OR name LIKE '%%%s%%'",
                    escape_string(users[client_id].db_conn, search_term),
                    escape_string(users[client_id].db_conn, search_term));
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                MYSQL_ROW row;
                char room_data[2048] = "";
                
                while ((row = mysql_fetch_row(result))) {
                    char temp[256];
                    sprintf(temp, "\r%s/%s/%s/%s/%s/%s/83.117.80.215/83.117.80.215/37120/%s/null/%s",
                            row[0], row[1], row[3], row[4], row[5], row[6], row[10], row[12]);
                    strcat(room_data, temp);
                }
                
                char busy_flats[2048];
                sprintf(busy_flats, "BUSY_FLAT_RESULTS 1%s", room_data);
                send_data(client_id, busy_flats);
                mysql_free_result(result);
            }
        }
    }
    else if (strcmp(token, "TRYFLAT") == 0) {
        char* flat_id = strchr(packet_copy, '/') + 1;
        if (flat_id) {
            char* password = strchr(flat_id, '/') + 1;
            if (password) {
                char* newline = strchr(password, '\r');
                if (newline) *newline = '\0';
                
                int gotoroom = atoi(flat_id);
                
                if (users[client_id].inroom != 0) {
                    char query[128];
                    sprintf(query, "UPDATE rooms SET inroom = inroom - 1 WHERE id = '%d'", users[client_id].inroom);
                    mysql_query(users[client_id].db_conn, query);
                    users[client_id].owner = 0;
                }
                
                char query[256];
                sprintf(query, "UPDATE users SET inroom = '%d' WHERE name = '%s'", 
                        gotoroom, escape_string(users[client_id].db_conn, users[client_id].name));
                mysql_query(users[client_id].db_conn, query);
                
                sprintf(query, "UPDATE rooms SET inroom = inroom + 1 WHERE id = '%d'", gotoroom);
                mysql_query(users[client_id].db_conn, query);
                
                users[client_id].inroom = gotoroom;
                send_data(client_id, "FLAT_LETIN");
            }
        }
    }
    else if (strcmp(token, "GOTOFLAT") == 0) {
        char* flat_id = strchr(packet_copy, '/') + 1;
        if (flat_id) {
            char* newline = strchr(flat_id, '\r');
            if (newline) *newline = '\0';
            
            int gotoroom = atoi(flat_id);
            
            char query[256];
            sprintf(query, "SELECT * FROM rooms WHERE id = '%d' LIMIT 1", gotoroom);
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                if (mysql_num_rows(result) > 0) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    
                    char room_ready[256];
                    sprintf(room_ready, "ROOM_READY\r%s", row[12]);
                    send_data(client_id, room_ready);
                    
                    char flat_prop1[128];
                    sprintf(flat_prop1, "FLATPROPERTY\rwallpaper/%s\r", row[8]);
                    send_data(client_id, flat_prop1);
                    
                    char flat_prop2[128];
                    sprintf(flat_prop2, "FLATPROPERTY\rfloor/%s\r", row[9]);
                    send_data(client_id, flat_prop2);
                    
                    if (strcmp(row[3], users[client_id].name) == 0) {
                        users[client_id].owner = 1;
                        send_data(client_id, "YOUAREOWNER");
                    }
                    
                    // Check rights
                    sprintf(query, "SELECT * FROM rights WHERE user = '%s' LIMIT 1", 
                            escape_string(users[client_id].db_conn, users[client_id].name));
                    if (mysql_query(users[client_id].db_conn, query) == 0) {
                        MYSQL_RES* rights_result = mysql_store_result(users[client_id].db_conn);
                        if (mysql_num_rows(rights_result) > 0) {
                            users[client_id].rights = 1;
                            send_data(client_id, "YOUARECONTROLLER");
                        }
                        mysql_free_result(rights_result);
                    }
                    
                    // Send heightmap
                    sprintf(query, "SELECT heightmap FROM heightmap WHERE model = '%s'", row[7]);
                    if (mysql_query(users[client_id].db_conn, query) == 0) {
                        MYSQL_RES* height_result = mysql_store_result(users[client_id].db_conn);
                        if (mysql_num_rows(height_result) > 0) {
                            MYSQL_ROW height_row = mysql_fetch_row(height_result);
                            strcpy(users[client_id].roomcache, height_row[0]);
                            
                            char heightmap_packet[2048];
                            sprintf(heightmap_packet, "HEIGHTMAP\r%s", height_row[0]);
                            send_data(client_id, heightmap_packet);
                        }
                        mysql_free_result(height_result);
                    }
                    
                    char objects[256];
                    sprintf(objects, "OBJECTS WORLD 0 %s", row[7]);
                    send_data(client_id, objects);
                    
                    // Send room items
                    sprintf(query, "SELECT * FROM roomitems WHERE roomid = '%d'", gotoroom);
                    if (mysql_query(users[client_id].db_conn, query) == 0) {
                        MYSQL_RES* items_result = mysql_store_result(users[client_id].db_conn);
                        MYSQL_ROW item_row;
                        char room_items[4096] = "ACTIVE_OBJECTS ";
                        
                        while ((item_row = mysql_fetch_row(items_result))) {
                            char temp[512];
                            sprintf(temp, "\r00000000000%s,%s %s %s %s %s 0.0 %s /%s/%s/",
                                    item_row[0], item_row[3], item_row[9], item_row[10], 
                                    item_row[7], item_row[11], item_row[8], item_row[4], item_row[5]);
                            strcat(room_items, temp);
                        }
                        
                        send_data(client_id, room_items);
                        mysql_free_result(items_result);
                    }
                    
                    send_data(client_id, "ITEMS \r6663\tposter\t \tfrontwall 6.9999,6.3500/21");
                    
                    // Send other users in room
                    sprintf(query, "SELECT * FROM users WHERE inroom = '%d' AND name != '%s'", 
                            gotoroom, escape_string(users[client_id].db_conn, users[client_id].name));
                    if (mysql_query(users[client_id].db_conn, query) == 0) {
                        MYSQL_RES* users_result = mysql_store_result(users[client_id].db_conn);
                        MYSQL_ROW user_row;
                        
                        while ((user_row = mysql_fetch_row(users_result))) {
                            char user_packet[256];
                            sprintf(user_packet, "USERS\r %s %s 3 5 0 %s",
                                    user_row[1], user_row[5] + 7, user_row[8]);
                            send_data(client_id, user_packet);
                        }
                        mysql_free_result(users_result);
                    }
                    
                    // Send current user status
                    if (users[client_id].owner || users[client_id].rights) {
                        char status_all[256];
                        sprintf(status_all, "STATUS \r%s 0,0,99,2,2/flatctrl useradmin/mod 0/", users[client_id].name);
                        send_data_all(status_all);
                        
                        char user_packet[256];
                        sprintf(user_packet, "USERS\r %s %s 3 5 0 %s",
                                users[client_id].name, users[client_id].figure + 7, users[client_id].customData);
                        send_data_room(gotoroom, user_packet);
                        
                        char status_room[256];
                        sprintf(status_room, "STATUS \r%s 3,5,0,2,2/flatctrl useradmin/mod 0/", users[client_id].name);
                        send_data_room(gotoroom, status_room);
                    } else {
                        char status_all[256];
                        sprintf(status_all, "STATUS \r%s 0,0,99,2,2/mod 0/", users[client_id].name);
                        send_data_all(status_all);
                        
                        char user_packet[256];
                        sprintf(user_packet, "USERS\r %s %s 3 5 0 %s",
                                users[client_id].name, users[client_id].figure + 7, users[client_id].customData);
                        send_data_room(gotoroom, user_packet);
                        
                        char status_room[256];
                        sprintf(status_room, "STATUS \r%s 3,5,0,2,2/mod 0/", users[client_id].name);
                        send_data_room(gotoroom, status_room);
                    }
                    
                    users[client_id].userx = 3;
                    users[client_id].usery = 5;
                }
                mysql_free_result(result);
            }
        }
    }
    else if (strcmp(token, "SHOUT") == 0 || strcmp(token, "CHAT") == 0 || strcmp(token, "WHISPER") == 0) {
        char* message = packet_copy + strlen(token) + 1; // Skip command and space
        
        // Remove quotes and special characters
        char clean_message[512];
        int j = 0;
        for (int i = 0; message[i] && i < 511; i++) {
            if (message[i] != '\'' && message[i] != '"' && message[i] != '#') {
                clean_message[j++] = message[i];
            }
        }
        clean_message[j] = '\0';
        
        // Filter message
        char filtered_message[512];
        strcpy(filtered_message, clean_message);
        filter_chat_message(filtered_message);
        
        // Send to room
        char chat_packet[1024];
        sprintf(chat_packet, "%s\r%s %s", token, users[client_id].name, filtered_message);
        send_data_room(users[client_id].inroom, chat_packet);
        
        // Log chat
        char log_query[1024];
        sprintf(log_query, "INSERT INTO chatloggs VALUES (NULL, '%s', '%s', '%d', '%s')",
                escape_string(users[client_id].db_conn, users[client_id].name),
                escape_string(users[client_id].db_conn, filtered_message),
                users[client_id].inroom, token);
        mysql_query(users[client_id].db_conn, log_query);
    }
    else if (strcmp(token, "STATUSOK") == 0) {
        send_data(client_id, "OK");
    }
    else if (strcmp(token, "MOVE") == 0) {
        char* x_str = strtok(NULL, " ");
        char* y_str = strtok(NULL, " ");
        
        if (x_str && y_str) {
            int to_x = atoi(x_str);
            int to_y = atoi(y_str);
            
            // Start pathfinding in separate thread
            pthread_mutex_lock(&users_mutex);
            users[client_id].target_x = to_x;
            users[client_id].target_y = to_y;
            users[client_id].pathfinding_active = 1;
            
            if (users[client_id].pathfinding_thread) {
                pthread_cancel(users[client_id].pathfinding_thread);
            }
            
            pthread_create(&users[client_id].pathfinding_thread, NULL, pathfinding_thread, &users[client_id].id);
            pthread_mutex_unlock(&users_mutex);
        }
    }
    else if (strcmp(token, "GOAWAY") == 0) {
        char status_packet[256];
        sprintf(status_packet, "STATUS \r%s 0,0,99,2,2/mod 0/", users[client_id].name);
        send_data_room(users[client_id].inroom, status_packet);
        
        users[client_id].userx = 0;
        users[client_id].usery = 0;
        users[client_id].dance = 0;
        
        if (users[client_id].inroom != 0) {
            char query[256];
            sprintf(query, "UPDATE users SET inroom = 0 WHERE name = '%s'", 
                    escape_string(users[client_id].db_conn, users[client_id].name));
            mysql_query(users[client_id].db_conn, query);
            
            sprintf(query, "UPDATE rooms SET inroom = inroom - 1 WHERE id = %d", users[client_id].inroom);
            mysql_query(users[client_id].db_conn, query);
            
            users[client_id].inroom = 0;
            users[client_id].owner = 0;
        }
        
        cleanup_user(client_id);
    }
    else if (strcmp(token, "DANCE") == 0) {
        char status_packet[256];
        sprintf(status_packet, "STATUS\r%s %d,%d,0,2,2/dance/", 
                users[client_id].name, users[client_id].userx, users[client_id].usery);
        send_data_room(users[client_id].inroom, status_packet);
        users[client_id].dance = 1;
    }
    else if (strcmp(token, "STOP") == 0) {
        char* stop_type = strtok(NULL, " ");
        if (stop_type && strcmp(stop_type, "DANCE") == 0) {
            char status_packet[256];
            sprintf(status_packet, "STATUS\r%s %d,%d,0,2,2/", 
                    users[client_id].name, users[client_id].userx, users[client_id].usery);
            send_data_room(users[client_id].inroom, status_packet);
            users[client_id].dance = 0;
        }
    }
    else if (strcmp(token, "GETORDERINFO") == 0) {
        char* item_name = strtok(NULL, " ");
        if (item_name) {
            char query[256];
            sprintf(query, "SELECT * FROM catalogue WHERE shortname = '%s' LIMIT 1", 
                    escape_string(users[client_id].db_conn, item_name));
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                if (mysql_num_rows(result) > 0) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    char order_info[256];
                    sprintf(order_info, "ORDERINFO\r%s/%s/\r%s", row[2], row[3], row[3]);
                    send_data(client_id, order_info);
                }
                mysql_free_result(result);
            }
        }
    }
    else if (strcmp(token, "GETSTRIP") == 0) {
        char* strip_type = strtok(NULL, " ");
        if (strip_type) {
            if (strcmp(strip_type, "new") == 0) {
                char query[256];
                sprintf(query, "SELECT * FROM useritems WHERE user = '%s' LIMIT 0,11", 
                        escape_string(users[client_id].db_conn, users[client_id].name));
                
                if (mysql_query(users[client_id].db_conn, query) == 0) {
                    MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                    MYSQL_ROW row;
                    char item_info[2048] = "";
                    
                    while ((row = mysql_fetch_row(result))) {
                        char temp[256];
                        sprintf(temp, "BLSI;%s;0;S;%s;%s;%s;%s;1;1;0,0,0/\r",
                                row[0], row[0], row[3], row[4], row[5]);
                        strcat(item_info, temp);
                    }
                    
                    users[client_id].handcache = 11;
                    char strip_info[2048];
                    sprintf(strip_info, "STRIPINFO\r%s", item_info);
                    send_data(client_id, strip_info);
                    mysql_free_result(result);
                }
            }
            else if (strcmp(strip_type, "next") == 0) {
                int start = users[client_id].handcache;
                int end = start + 11;
                
                char query[256];
                sprintf(query, "SELECT * FROM useritems WHERE user = '%s' LIMIT %d,%d", 
                        escape_string(users[client_id].db_conn, users[client_id].name), start, end);
                
                if (mysql_query(users[client_id].db_conn, query) == 0) {
                    MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                    MYSQL_ROW row;
                    char item_info[2048] = "";
                    
                    while ((row = mysql_fetch_row(result))) {
                        char temp[256];
                        sprintf(temp, "BLSI;%s;0;S;%s;%s;%s;%s;1;1;0,0,0/\r",
                                row[0], row[0], row[3], row[4], row[5]);
                        strcat(item_info, temp);
                    }
                    
                    users[client_id].handcache = end;
                    char strip_info[2048];
                    sprintf(strip_info, "STRIPINFO\r%s", item_info);
                    send_data(client_id, strip_info);
                    mysql_free_result(result);
                }
            }
        }
    }
    else if (strcmp(token, "PURCHASE") == 0) {
        char* item_name = strchr(packet_copy, '/') + 1;
        if (item_name) {
            char* price_str = strchr(item_name, '/') + 1;
            if (price_str) {
                char* newline = strchr(price_str, '\r');
                if (newline) *newline = '\0';
                
                char query[256];
                sprintf(query, "SELECT * FROM catalogue WHERE longname = '%s' LIMIT 1", 
                        escape_string(users[client_id].db_conn, item_name));
                
                if (mysql_query(users[client_id].db_conn, query) == 0) {
                    MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                    if (mysql_num_rows(result) > 0) {
                        MYSQL_ROW row = mysql_fetch_row(result);
                        int price = atoi(price_str);
                        
                        char insert_query[512];
                        sprintf(insert_query, "INSERT INTO useritems VALUES (NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                                escape_string(users[client_id].db_conn, users[client_id].name),
                                row[2], row[3], row[4], row[5], row[6], row[1]);
                        mysql_query(users[client_id].db_conn, insert_query);
                        
                        sprintf(query, "UPDATE users SET credits = credits - %d WHERE name = '%s'",
                                price, escape_string(users[client_id].db_conn, users[client_id].name));
                        mysql_query(users[client_id].db_conn, query);
                        
                        users[client_id].credits -= price;
                        send_data(client_id, "SYSTEMBROADCAST\rBuying successful !");
                    }
                    mysql_free_result(result);
                }
            }
        }
    }
    else if (strcmp(token, "CREATEFLAT") == 0) {
        char* flat_data = strchr(packet_copy, '/') + 1;
        if (flat_data) {
            char* name = flat_data;
            char* model = strchr(name, '/') + 1;
            char* door = strchr(model, '/') + 1;
            char* password = strchr(door, '/') + 1;
            
            if (password) {
                char* newline = strchr(password, '\r');
                if (newline) *newline = '\0';
                
                char query[512];
                sprintf(query, "INSERT INTO rooms VALUES (NULL, '%s', '', '%s', 'floor1', '0', '%s', '%s', '', '100', '100')",
                        escape_string(users[client_id].db_conn, name),
                        escape_string(users[client_id].db_conn, users[client_id].name),
                        escape_string(users[client_id].db_conn, door),
                        escape_string(users[client_id].db_conn, password));
                mysql_query(users[client_id].db_conn, query);
            }
        }
    }
    else if (strcmp(token, "MESSENGER_REQUESTBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            
            char query[256];
            sprintf(query, "INSERT INTO buddy VALUES (NULL, '%s', '%s', '0')",
                    escape_string(users[client_id].db_conn, users[client_id].name),
                    escape_string(users[client_id].db_conn, buddy_name));
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "MESSENGER_ACCEPTBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            
            char query[256];
            sprintf(query, "UPDATE buddy SET accept = '1' WHERE `from` LIKE '%s' AND `to` = '%s' AND `accept` = '0'",
                    escape_string(users[client_id].db_conn, buddy_name),
                    escape_string(users[client_id].db_conn, users[client_id].name));
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "MESSENGER_DECLINEBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            
            char query[256];
            sprintf(query, "DELETE FROM buddy WHERE `from` LIKE '%s' AND `to` = '%s' AND `accept` = '0'",
                    escape_string(users[client_id].db_conn, buddy_name),
                    escape_string(users[client_id].db_conn, users[client_id].name));
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "MESSENGERINIT") == 0) {
        char query[256];
        sprintf(query, "SELECT * FROM buddy WHERE `to` LIKE '%s' AND `accept` = '1' OR `from` LIKE '%s' AND `accept` = '1'",
                escape_string(users[client_id].db_conn, users[client_id].name),
                escape_string(users[client_id].db_conn, users[client_id].name));
        
        if (mysql_query(users[client_id].db_conn, query) == 0) {
            MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
            MYSQL_ROW row;
            char buddy_data[1024] = "";
            
            while ((row = mysql_fetch_row(result))) {
                char* data_name = (strcmp(row[1], users[client_id].name) != 0) ? row[1] : row[2];
                char temp[128];
                sprintf(temp, "\r1 %s UNKNOW\r", data_name);
                strcat(buddy_data, temp);
            }
            
            char buddy_list[1024];
            sprintf(buddy_list, "BUDDYLIST%s", buddy_data);
            send_data(client_id, buddy_list);
            mysql_free_result(result);
        }
    }
    else if (strcmp(token, "MESSENGER_REMOVEBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            
            char query[512];
            sprintf(query, "DELETE FROM buddy WHERE `from` LIKE '%s' AND `to` = '%s' AND `accept` = '1' LIMIT 1",
                    escape_string(users[client_id].db_conn, buddy_name),
                    escape_string(users[client_id].db_conn, users[client_id].name));
            mysql_query(users[client_id].db_conn, query);
            
            sprintf(query, "DELETE FROM buddy WHERE `to` LIKE '%s' AND `from` = '%s' AND `accept` = '1' LIMIT 1",
                    escape_string(users[client_id].db_conn, buddy_name),
                    escape_string(users[client_id].db_conn, users[client_id].name));
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "PLACESTUFFFROMSTRIP") == 0) {
        char* item_id = strtok(NULL, " ");
        char* x_str = strtok(NULL, " ");
        char* y_str = strtok(NULL, " ");
        
        if (item_id && x_str && y_str) {
            char query[512];
            sprintf(query, "SELECT * FROM useritems WHERE id = '%s' LIMIT 1", item_id);
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                if (mysql_num_rows(result) > 0) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    
                    char insert_query[512];
                    sprintf(insert_query, "INSERT INTO roomitems VALUES (NULL, '%d', '%s', '%s', '%s', '%s', '%s', '%s','0' , '%s', '%s')",
                            users[client_id].inroom, row[3], row[4], row[5], row[6], row[7], row[8], x_str, y_str);
                    mysql_query(users[client_id].db_conn, insert_query);
                }
                mysql_free_result(result);
            }
            
            sprintf(query, "DELETE FROM useritems WHERE id = '%s'", item_id);
            mysql_query(users[client_id].db_conn, query);
            
            sprintf(query, "SELECT * FROM roomitems WHERE roomid = '%d' ORDER BY id DESC LIMIT 1", users[client_id].inroom);
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                if (mysql_num_rows(result) > 0) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    char room_items[512];
                    sprintf(room_items, "ACTIVE_OBJECTS \r00000000000%s,%s %s %s %s %s 0.0 %s /%s/%s/",
                            row[0], row[3], row[9], row[10], row[7], row[11], row[8], row[4], row[5]);
                    send_data_room(users[client_id].inroom, room_items);
                }
                mysql_free_result(result);
            }
        }
    }
    else if (strcmp(token, "MOVESTUFF") == 0) {
        char* item_id = strtok(NULL, " ");
        char* x_str = strtok(NULL, " ");
        char* y_str = strtok(NULL, " ");
        char* rotate_str = strtok(NULL, " ");
        
        if (item_id && x_str && y_str && rotate_str) {
            char query[256];
            sprintf(query, "UPDATE roomitems SET x = '%s', y = '%s', rotate = '%s' WHERE id = '%s' LIMIT 1",
                    x_str, y_str, rotate_str, item_id);
            mysql_query(users[client_id].db_conn, query);
            
            sprintf(query, "SELECT * FROM roomitems WHERE roomid = '%d' AND id = '%s'", 
                    users[client_id].inroom, item_id);
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                if (mysql_num_rows(result) > 0) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    char room_items[512];
                    sprintf(room_items, "ACTIVEOBJECT_UPDATE \r00000000000%s,%s %s %s %s %s 0.0 %s /%s/%s/",
                            row[0], row[3], row[9], row[10], row[7], row[11], row[8], row[4], row[5]);
                    send_data_room(users[client_id].inroom, room_items);
                }
                mysql_free_result(result);
            }
        }
    }
    else if (strcmp(token, "REMOVESTUFF") == 0) {
        char* item_id = strtok(NULL, " ");
        if (item_id) {
            char query[256];
            sprintf(query, "SELECT * FROM roomitems WHERE roomid = '%d' AND id = '%s' LIMIT 1", 
                    users[client_id].inroom, item_id);
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                if (mysql_num_rows(result) > 0) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    char room_items[512];
                    sprintf(room_items, "ACTIVEOBJECT_UPDATE \r00000000000%s,%s 99 99 %s %s 0.0 %s /%s/%s/",
                            row[0], row[3], row[7], row[11], row[8], row[4], row[5]);
                    send_data_room(users[client_id].inroom, room_items);
                }
                mysql_free_result(result);
            }
            
            sprintf(query, "DELETE FROM roomitems WHERE id = '%s' LIMIT 1", item_id);
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "ADDSTRIPITEM") == 0) {
        char* item_id = strtok(NULL, " ");
        strtok(NULL, " "); // Skip parameter
        char* room_item_id = strtok(NULL, " ");
        
        if (item_id && room_item_id) {
            char query[256];
            sprintf(query, "SELECT * FROM roomitems WHERE roomid = '%d' AND id = '%s' LIMIT 1", 
                    users[client_id].inroom, room_item_id);
            
            if (mysql_query(users[client_id].db_conn, query) == 0) {
                MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
                if (mysql_num_rows(result) > 0) {
                    MYSQL_ROW row = mysql_fetch_row(result);
                    char room_items[512];
                    sprintf(room_items, "ACTIVEOBJECT_UPDATE \r00000000000%s,%s 99 99 %s %s 0.0 %s /%s/%s/",
                            row[0], row[3], row[7], row[11], row[8], row[4], row[5]);
                    send_data_room(users[client_id].inroom, room_items);
                    
                    char insert_query[512];
                    sprintf(insert_query, "INSERT INTO useritems VALUES (NULL, '%s', '%s', '%s', '%s', '%s', '%s', '%s')",
                            escape_string(users[client_id].db_conn, users[client_id].name),
                            row[3], row[4], row[5], row[6], row[7], row[8]);
                    mysql_query(users[client_id].db_conn, insert_query);
                    
                    sprintf(query, "DELETE FROM roomitems WHERE id = '%s' LIMIT 1", room_item_id);
                    mysql_query(users[client_id].db_conn, query);
                }
                mysql_free_result(result);
            }
        }
    }
    else if (strcmp(token, "LOOKTO") == 0) {
        char* x_str = strtok(NULL, " ");
        char* y_str = strtok(NULL, " ");
        
        if (x_str && y_str) {
            int to_x = atoi(x_str);
            int to_y = atoi(y_str);
            
            char own_data[50] = "";
            if (users[client_id].owner) {
                strcpy(own_data, "flatctrl useradmin/");
            }
            
            // Calculate rotation
            int rx = 2, ry = 2; // Default
            if (users[client_id].userx > to_x && users[client_id].usery > to_y) {
                rx = 7; ry = 7; // Top-left
            } else if (users[client_id].userx > to_x && users[client_id].usery < to_y) {
                rx = 5; ry = 5; // Bottom-left
            } else if (users[client_id].userx < to_x && users[client_id].usery > to_y) {
                rx = 1; ry = 1; // Top-right
            } else if (users[client_id].userx < to_x && users[client_id].usery < to_y) {
                rx = 3; ry = 3; // Bottom-right
            } else if (users[client_id].userx > to_x) {
                rx = 6; ry = 6; // Left
            } else if (users[client_id].userx < to_x) {
                rx = 2; ry = 2; // Right
            } else if (users[client_id].usery > to_y) {
                rx = 0; ry = 0; // Up
            } else if (users[client_id].usery < to_y) {
                rx = 4; ry = 4; // Down
            }
            
            char status_packet[256];
            if (users[client_id].dance) {
                sprintf(status_packet, "STATUS\r%s %d,%d,%d,%d,%d/%sdance/", 
                        users[client_id].name, users[client_id].userx, users[client_id].usery, 
                        users[client_id].userz, rx, ry, own_data);
            } else {
                sprintf(status_packet, "STATUS\r%s %d,%d,%d,%d,%d/%s", 
                        users[client_id].name, users[client_id].userx, users[client_id].usery, 
                        users[client_id].userz, rx, ry, own_data);
            }
            send_data_room(users[client_id].inroom, status_packet);
            
            users[client_id].userrx = rx;
            users[client_id].userry = ry;
        }
    }
    else if (strcmp(token, "KILLUSER") == 0) {
        char* target_user = strtok(NULL, " ");
        if (target_user) {
            char* newline = strchr(target_user, '\r');
            if (newline) *newline = '\0';
            
            pthread_mutex_lock(&users_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (users[i].used && strcmp(users[i].name, target_user) == 0 && 
                    users[i].inroom == users[client_id].inroom) {
                    send_data(i, "SYSTEMBROADCAST\rYou are kicked out of the room!");
                    cleanup_user(i);
                    break;
                }
            }
            pthread_mutex_unlock(&users_mutex);
        }
    }
    else if (strcmp(token, "ASSIGNRIGHTS") == 0) {
        char* target_user = strtok(NULL, " ");
        if (target_user) {
            char* newline = strchr(target_user, '\r');
            if (newline) *newline = '\0';
            
            char query[256];
            sprintf(query, "INSERT INTO rights VALUES (NULL, '%d', '%s')", 
                    users[client_id].inroom, escape_string(users[client_id].db_conn, target_user));
            mysql_query(users[client_id].db_conn, query);
            
            pthread_mutex_lock(&users_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (users[i].used && strcmp(users[i].name, target_user) == 0 && 
                    users[i].inroom == users[client_id].inroom) {
                    send_data(i, "YOUARECONTROLLER");
                    users[i].rights = 1;
                    break;
                }
            }
            pthread_mutex_unlock(&users_mutex);
        }
    }
    else if (strcmp(token, "REMOVERIGHTS") == 0) {
        char* target_user = strtok(NULL, " ");
        if (target_user) {
            char* newline = strchr(target_user, '\r');
            if (newline) *newline = '\0';
            
            char query[256];
            sprintf(query, "DELETE FROM rights WHERE room = '%d' AND user = '%s'", 
                    users[client_id].inroom, escape_string(users[client_id].db_conn, target_user));
            mysql_query(users[client_id].db_conn, query);
        }
    }
    else if (strcmp(token, "UPDATEFLAT") == 0) {
        char* flat_data = strchr(packet_copy, '/') + 1;
        if (flat_data) {
            char* flat_id = flat_data;
            char* name = strchr(flat_id, '/') + 1;
            char* door = strchr(name, '/') + 1;
            
            if (door) {
                char* newline = strchr(door, '\r');
                if (newline) *newline = '\0';
                
                char query[512];
                sprintf(query, "UPDATE rooms SET name = '%s', door = '%s' WHERE id = '%s' LIMIT 1",
                        escape_string(users[client_id].db_conn, name),
                        escape_string(users[client_id].db_conn, door),
                        flat_id);
                mysql_query(users[client_id].db_conn, query);
            }
        }
    }
    else if (strcmp(token, "SETFLATINFO") == 0) {
        char* flat_data = strchr(packet_copy, '/') + 1;
        if (flat_data) {
            char* flat_id = flat_data;
            char* info_data = strchr(flat_id, '/') + 1;
            
            if (info_data) {
                char* newline = strchr(info_data, '\r');
                if (newline) *newline = '\0';
                
                char desc[256], password[100];
                sscanf(info_data, "desc=%[^/]/password=%s", desc, password);
                
                char query[512];
                sprintf(query, "UPDATE rooms SET desc = '%s', pass = '%s' WHERE id = '%s'",
                        escape_string(users[client_id].db_conn, desc),
                        escape_string(users[client_id].db_conn, password),
                        flat_id);
                mysql_query(users[client_id].db_conn, query);
            }
        }
    }
}

void load_user_data(int client_id, const char* username, const char* password) {
    char query[512];
    sprintf(query, "SELECT * FROM users WHERE name = '%s' AND password = '%s'", 
            escape_string(users[client_id].db_conn, username),
            escape_string(users[client_id].db_conn, password));
    
    if (mysql_query(users[client_id].db_conn, query) == 0) {
        MYSQL_RES* result = mysql_store_result(users[client_id].db_conn);
        if (mysql_num_rows(result) > 0) {
            MYSQL_ROW row = mysql_fetch_row(result);
            
            strcpy(users[client_id].name, row[1]);
            strcpy(users[client_id].password, row[2]);
            users[client_id].credits = atoi(row[3]);
            strcpy(users[client_id].email, row[4]);
            strcpy(users[client_id].figure, row[5]);
            strcpy(users[client_id].birthday, row[6]);
            strcpy(users[client_id].phonenumber, row[7]);
            strcpy(users[client_id].customData, row[8]);
            users[client_id].had_read_agreement = atoi(row[9]);
            strcpy(users[client_id].sex, row[10]);
            strcpy(users[client_id].country, row[11]);
            strcpy(users[client_id].has_special_rights, row[12]);
            strcpy(users[client_id].badge_type, row[13]);
            
            // Load inroom if not set
            if (users[client_id].inroom == 0) {
                char inroom_query[256];
                sprintf(inroom_query, "SELECT inroom FROM users WHERE name = '%s'", 
                        escape_string(users[client_id].db_conn, username));
                if (mysql_query(users[client_id].db_conn, inroom_query) == 0) {
                    MYSQL_RES* inroom_result = mysql_store_result(users[client_id].db_conn);
                    if (mysql_num_rows(inroom_result) > 0) {
                        MYSQL_ROW inroom_row = mysql_fetch_row(inroom_result);
                        users[client_id].inroom = atoi(inroom_row[0]);
                    }
                    mysql_free_result(inroom_result);
                }
            }
        }
        mysql_free_result(result);
    }
}

void send_data(int client_id, const char* data) {
    if (client_id < 0 || client_id >= MAX_CLIENTS) return;
    if (!users[client_id].used || users[client_id].socket == INVALID_SOCKET) return;
    send_cmd(&users[client_id], data);
}

void send_data_all(const char* data) {
    pthread_mutex_lock(&users_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (users[i].used && users[i].socket != INVALID_SOCKET) {
            send_cmd(&users[i], data);
        }
    }
    pthread_mutex_unlock(&users_mutex);
}

void send_data_room(int room_id, const char* data) {
    pthread_mutex_lock(&users_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (users[i].used && users[i].inroom == room_id && users[i].socket != INVALID_SOCKET) {
            send_cmd(&users[i], data);
        }
    }
    pthread_mutex_unlock(&users_mutex);
}

void load_wordfilters() {
    // This would load from database in a real implementation
}

char* filter_chat_message(char* message) {
    // This would filter based on database word filters
    return message;
}

void* pathfinding_thread(void* arg) {
    int client_id = *(int*)arg;
    
    pthread_mutex_lock(&users_mutex);
    int target_x = users[client_id].target_x;
    int target_y = users[client_id].target_y;
    pthread_mutex_unlock(&users_mutex);
    
    while (1) {
        pthread_mutex_lock(&users_mutex);
        if (!users[client_id].pathfinding_active || !users[client_id].used) {
            pthread_mutex_unlock(&users_mutex);
            break;
        }
        
        int current_x = users[client_id].userx;
        int current_y = users[client_id].usery;
        pthread_mutex_unlock(&users_mutex);
        
        // Check if reached target
        if (current_x == target_x && current_y == target_y) {
            pthread_mutex_lock(&users_mutex);
            users[client_id].pathfinding_active = 0;
            pthread_mutex_unlock(&users_mutex);
            break;
        }
        
        // Calculate next step
        int new_x = current_x;
        int new_y = current_y;
        
        if (current_x < target_x) new_x++;
        else if (current_x > target_x) new_x--;
        
        if (current_y < target_y) new_y++;
        else if (current_y > target_y) new_y--;
        
        // Get height at new position
        int height = get_room_height(new_x, new_y, users[client_id].roomcache);
        
        // Send status update
        char status_packet[256];
        char own_data[50] = "";
        pthread_mutex_lock(&users_mutex);
        if (users[client_id].owner || users[client_id].rights) {
            strcpy(own_data, "flatctrl useradmin/");
        }
        pthread_mutex_unlock(&users_mutex);
        
        sprintf(status_packet, "STATUS \r%s %d,%d,%d,2,2/%s", 
                users[client_id].name, new_x, new_y, height, own_data);
        send_data_room(users[client_id].inroom, status_packet);
        
        // Update user position
        pthread_mutex_lock(&users_mutex);
        users[client_id].userx = new_x;
        users[client_id].usery = new_y;
        users[client_id].userz = height;
        pthread_mutex_unlock(&users_mutex);
        
        // Sleep for movement delay
        usleep(500000); // 500ms
    }
    
    return NULL;
}

int get_room_height(int x, int y, char* heightmap) {
    // Simple height calculation based on position
    // In a real implementation, this would parse the actual heightmap
    if (x >= 0 && x < 10 && y >= 0 && y < 28) {
        return 1; // Default height
    }
    return 0;
}

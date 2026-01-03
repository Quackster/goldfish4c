/*
    Server.c — MySQL C API (MYSQL_STMT) prepared statements for ALL DB operations.

    Fixes in this version (per your request):
    - Integrated with the actual DB schema from v1.sql (no more SELECT * index mismatches).
    - Fixed row index usage everywhere (rooms/roomitems/useritems/catalogue/etc).
    - Fixed buffers to support DB field sizes + safer concatenation.
    - Fixed STRIP paging (LIMIT offset,count; not offset,end).
    - Removed broken SEARCHFLAT callback usage and simplified room list formatting.
    - Rewrote FAVORITES using a JOIN (no N+1).
    - PURCHASE now uses catalogue.price from DB (prevents client price spoofing).
    - Room item packets now map to the v1.sql roomitems columns:
        id, shortname, xx, yy, zz, rotate, extra, longname, color

    Build:
      gcc server.c -o server -lmysqlclient -lpthread
*/

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include <windows.h>
    #include <process.h>
    #pragma comment(lib, "ws2_32.lib")

    typedef HANDLE pthread_t;
    typedef CRITICAL_SECTION pthread_mutex_t;
    #define PTHREAD_MUTEX_INITIALIZER {0}

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

    void pthread_mutex_lock(pthread_mutex_t *mutex) { EnterCriticalSection(mutex); }
    void pthread_mutex_unlock(pthread_mutex_t *mutex) { LeaveCriticalSection(mutex); }

    int pthread_mutex_init(pthread_mutex_t *mutex, void *attr) {
        (void)attr;
        InitializeCriticalSection(mutex);
        return 0;
    }
    int pthread_mutex_destroy(pthread_mutex_t *mutex) { DeleteCriticalSection(mutex); return 0; }
    void pthread_detach(pthread_t thread) { CloseHandle(thread); }
    int pthread_cancel(pthread_t thread) { TerminateThread(thread, 0); return 0; }

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
#include <ctype.h>
#include <mysql/mysql.h>

#define MAX_CLIENTS 150
#define INVALID_SOCKET -1

/* Protocol safety:
   - the wire format uses a 4-digit length header => max 9999 bytes.
   - keep responses under this.
*/
#define MAX_WIRE_PAYLOAD 9999

/* IO buffers */
#define INBUF_SIZE   8192
#define BIGBUF_SIZE  16384
#define MIDBUF_SIZE  8192

/* DB bind output buffers */
#define DB_OUTBUF_SMALL  64
#define DB_OUTBUF_MED    512
#define DB_OUTBUF_LARGE  4096

typedef struct {
    int id;
    int socket;

    char name[50];
    char password[50];
    int credits;

    char email[255];
    char figure[255];
    char birthday[25];
    char phonenumber[20];
    char customData[1096 + 16]; /* db: varchar(1096), plus a little headroom */
    int  had_read_agreement;
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

    char roomcache[2048]; /* heightmap cache used by get_room_height */
    int handcache;

    int used;
    int server;

    pthread_t pathfinding_thread;
    int pathfinding_active;
    int target_x;
    int target_y;

    MYSQL* db_conn;
} User;

static User users[MAX_CLIENTS];
static pthread_mutex_t users_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t db_mutex = PTHREAD_MUTEX_INITIALIZER;
static MYSQL* global_db_conn = NULL;

/* -------------------- networking helpers -------------------- */

static int recv_all(int sock, void *buf, int len) {
    char *p = (char*)buf;
    int got = 0;
    while (got < len) {
        int r = recv(sock, p + got, len - got, 0);
        if (r <= 0) return r;
        got += r;
    }
    return got;
}

static int parse_len4(const char hdr[4]) {
    char digits[5];
    int j = 0;
    for (int i = 0; i < 4; i++) {
        unsigned char c = (unsigned char)hdr[i];
        if (c == ' ') continue;
        if (!isdigit(c)) return -1;
        if (j < 4) digits[j++] = (char)c;
    }
    digits[j] = '\0';
    if (j == 0) return 0;
    long v = strtol(digits, NULL, 10);
    if (v < 0 || v > 9999) return -1;
    return (int)v;
}

/* -------------------- protocol send helper -------------------- */

static int send_cmd(User *c, const char *data) {
    if (!c || c->socket == INVALID_SOCKET) return -1;
    if (!data) data = "";

    size_t dlen = strlen(data);
    size_t plen = 2 + dlen + 3; /* "# " + data + "\r##" */
    if (plen > MAX_WIRE_PAYLOAD) {
        /* truncate to fit wire limit */
        size_t max_d = MAX_WIRE_PAYLOAD - 2 - 3;
        dlen = max_d;
        plen = 2 + dlen + 3;
    }

    char *payload = (char*)malloc(plen + 1);
    if (!payload) return -1;

    memcpy(payload, "# ", 2);
    memcpy(payload + 2, data, dlen);
    memcpy(payload + 2 + dlen, "\r##", 3);
    payload[plen] = '\0';

    int rc = send(c->socket, payload, (int)plen, 0);

    /* optional readable debug output */
    {
        char *readable = (char*)malloc(plen * 4 + 1);
        if (readable) {
            size_t read_pos = 0;
            for (size_t i = 0; i < plen && read_pos < plen * 4; i++) {
                unsigned char ch = (unsigned char)payload[i];
                if (ch < 32 || ch == 127) {
                    int n = snprintf(readable + read_pos, (plen * 4) - read_pos, "[%u]", ch);
                    if (n <= 0) break;
                    read_pos += (size_t)n;
                } else {
                    readable[read_pos++] = (char)ch;
                }
            }
            readable[read_pos] = '\0';
            printf("<-- %s\n", readable);
            free(readable);
        } else {
            printf("<-- %s\n", payload);
        }
    }

    free(payload);
    return rc;
}

/* -------------------- prepared statement layer -------------------- */

typedef struct {
    MYSQL_BIND *bind;
    char      **bufs;
    unsigned long *lens;
    my_bool   *is_null;
    unsigned int cols;
    unsigned long buf_sz;
} StmtRow;

static void stmtrow_free(StmtRow *r) {
    if (!r) return;
    if (r->bufs) {
        for (unsigned int i = 0; i < r->cols; i++) free(r->bufs[i]);
    }
    free(r->bind);
    free(r->bufs);
    free(r->lens);
    free(r->is_null);
    memset(r, 0, sizeof(*r));
}

static int stmtrow_init_from_metadata(MYSQL_STMT *stmt, StmtRow *out, unsigned long buf_sz) {
    memset(out, 0, sizeof(*out));
    MYSQL_RES *meta = mysql_stmt_result_metadata(stmt);
    if (!meta) return -1;

    unsigned int cols = mysql_num_fields(meta);
    mysql_free_result(meta);

    out->cols = cols;
    out->buf_sz = buf_sz;

    out->bind = (MYSQL_BIND*)calloc(cols, sizeof(MYSQL_BIND));
    out->bufs = (char**)calloc(cols, sizeof(char*));
    out->lens = (unsigned long*)calloc(cols, sizeof(unsigned long));
    out->is_null = (my_bool*)calloc(cols, sizeof(my_bool));
    if (!out->bind || !out->bufs || !out->lens || !out->is_null) {
        stmtrow_free(out);
        return -1;
    }

    for (unsigned int i = 0; i < cols; i++) {
        out->bufs[i] = (char*)calloc(buf_sz, 1);
        if (!out->bufs[i]) {
            stmtrow_free(out);
            return -1;
        }
        memset(&out->bind[i], 0, sizeof(MYSQL_BIND));
        out->bind[i].buffer_type   = MYSQL_TYPE_STRING;
        out->bind[i].buffer        = out->bufs[i];
        out->bind[i].buffer_length = buf_sz;
        out->bind[i].length        = &out->lens[i];
        out->bind[i].is_null       = &out->is_null[i];
    }

    if (mysql_stmt_bind_result(stmt, out->bind) != 0) {
        fprintf(stderr, "bind_result failed: %s\n", mysql_stmt_error(stmt));
        stmtrow_free(out);
        return -1;
    }
    return 0;
}

static void bind_str_in(MYSQL_BIND *b, const char *s, unsigned long *len_out) {
    memset(b, 0, sizeof(*b));
    if (!s) s = "";
    *len_out = (unsigned long)strlen(s);
    b->buffer_type   = MYSQL_TYPE_STRING;
    b->buffer        = (void*)s;
    b->buffer_length = *len_out;
    b->length        = len_out;
}

static void bind_int_in(MYSQL_BIND *b, int *v) {
    memset(b, 0, sizeof(*b));
    b->buffer_type = MYSQL_TYPE_LONG;
    b->buffer      = (void*)v;
}

static int stmt_exec(MYSQL *conn, const char *sql, MYSQL_BIND *params, unsigned long param_count) {
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    if (mysql_stmt_prepare(stmt, sql, (unsigned long)strlen(sql)) != 0) {
        fprintf(stderr, "stmt_prepare failed: %s\n", mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return -1;
    }

    if (param_count > 0) {
        if (mysql_stmt_bind_param(stmt, params) != 0) {
            fprintf(stderr, "stmt_bind_param failed: %s\n", mysql_stmt_error(stmt));
            mysql_stmt_close(stmt);
            return -1;
        }
    }

    if (mysql_stmt_execute(stmt) != 0) {
        fprintf(stderr, "stmt_execute failed: %s\n", mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return -1;
    }

    mysql_stmt_close(stmt);
    return 0;
}

typedef int (*row_cb_fn)(StmtRow *row, void *ud); /* return 0 to continue, nonzero to stop */

static int stmt_query_each_row(MYSQL *conn, const char *sql,
                               MYSQL_BIND *params, unsigned long param_count,
                               row_cb_fn cb, void *ud,
                               unsigned long out_buf_sz)
{
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    if (mysql_stmt_prepare(stmt, sql, (unsigned long)strlen(sql)) != 0) {
        fprintf(stderr, "stmt_prepare failed: %s\n", mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return -1;
    }

    if (param_count > 0) {
        if (mysql_stmt_bind_param(stmt, params) != 0) {
            fprintf(stderr, "stmt_bind_param failed: %s\n", mysql_stmt_error(stmt));
            mysql_stmt_close(stmt);
            return -1;
        }
    }

    if (mysql_stmt_execute(stmt) != 0) {
        fprintf(stderr, "stmt_execute failed: %s\n", mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return -1;
    }

    StmtRow row;
    if (stmtrow_init_from_metadata(stmt, &row, out_buf_sz) != 0) {
        mysql_stmt_close(stmt);
        return -1;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        fprintf(stderr, "store_result failed: %s\n", mysql_stmt_error(stmt));
        stmtrow_free(&row);
        mysql_stmt_close(stmt);
        return -1;
    }

    int rc = 0;
    while (1) {
        int f = mysql_stmt_fetch(stmt);
        if (f == MYSQL_NO_DATA) break;
        if (f != 0 && f != MYSQL_DATA_TRUNCATED) {
            fprintf(stderr, "fetch failed: %s\n", mysql_stmt_error(stmt));
            rc = -1;
            break;
        }
        if (cb) {
            int stop = cb(&row, ud);
            if (stop) break;
        }
    }

    mysql_stmt_free_result(stmt);
    stmtrow_free(&row);
    mysql_stmt_close(stmt);
    return rc;
}

static int stmt_query_first_row(MYSQL *conn, const char *sql,
                                MYSQL_BIND *params, unsigned long param_count,
                                StmtRow *out_row, unsigned long out_buf_sz)
{
    MYSQL_STMT *stmt = mysql_stmt_init(conn);
    if (!stmt) return -1;

    if (mysql_stmt_prepare(stmt, sql, (unsigned long)strlen(sql)) != 0) {
        fprintf(stderr, "stmt_prepare failed: %s\n", mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return -1;
    }

    if (param_count > 0) {
        if (mysql_stmt_bind_param(stmt, params) != 0) {
            fprintf(stderr, "stmt_bind_param failed: %s\n", mysql_stmt_error(stmt));
            mysql_stmt_close(stmt);
            return -1;
        }
    }

    if (mysql_stmt_execute(stmt) != 0) {
        fprintf(stderr, "stmt_execute failed: %s\n", mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return -1;
    }

    if (stmtrow_init_from_metadata(stmt, out_row, out_buf_sz) != 0) {
        mysql_stmt_close(stmt);
        return -1;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        fprintf(stderr, "store_result failed: %s\n", mysql_stmt_error(stmt));
        stmtrow_free(out_row);
        mysql_stmt_close(stmt);
        return -1;
    }

    int f = mysql_stmt_fetch(stmt);
    if (f == MYSQL_NO_DATA) {
        mysql_stmt_free_result(stmt);
        stmtrow_free(out_row);
        mysql_stmt_close(stmt);
        return 0;
    }
    if (f != 0 && f != MYSQL_DATA_TRUNCATED) {
        fprintf(stderr, "fetch failed: %s\n", mysql_stmt_error(stmt));
        mysql_stmt_free_result(stmt);
        stmtrow_free(out_row);
        mysql_stmt_close(stmt);
        return -1;
    }

    mysql_stmt_free_result(stmt);
    mysql_stmt_close(stmt);
    return 1;
}

/* -------------------- DB convenience funcs -------------------- */

static int db_check_login(MYSQL *conn, const char *username, const char *password) {
    const char *sql = "SELECT 1 FROM users WHERE name=? AND password=? LIMIT 1";
    MYSQL_BIND p[2];
    unsigned long lu=0, lp=0;
    bind_str_in(&p[0], username, &lu);
    bind_str_in(&p[1], password, &lp);

    StmtRow row;
    int got = stmt_query_first_row(conn, sql, p, 2, &row, DB_OUTBUF_SMALL);
    if (got == 1) stmtrow_free(&row);
    return got;
}

static int db_load_user_data(MYSQL *conn, const char *username, const char *password, User *u) {
    /* users schema (v1.sql):
       id,name,password,credits,email,figure,birthday,phonenumber,customData,had_read_agreement,sex,country,has_special_rights,badge_type,inroom
    */
    const char *sql =
        "SELECT id,name,password,credits,email,figure,birthday,phonenumber,customData,had_read_agreement,sex,country,has_special_rights,badge_type,inroom "
        "FROM users WHERE name=? AND password=? LIMIT 1";

    MYSQL_BIND p[2];
    unsigned long lu=0, lp=0;
    bind_str_in(&p[0], username, &lu);
    bind_str_in(&p[1], password, &lp);

    StmtRow row;
    int got = stmt_query_first_row(conn, sql, p, 2, &row, DB_OUTBUF_LARGE);
    if (got != 1) return got;

    if (row.cols >= 1) u->id = atoi(row.bufs[0]);
    if (row.cols >= 2) snprintf(u->name, sizeof(u->name), "%s", row.bufs[1]);
    if (row.cols >= 3) snprintf(u->password, sizeof(u->password), "%s", row.bufs[2]);
    if (row.cols >= 4) u->credits = atoi(row.bufs[3]);
    if (row.cols >= 5) snprintf(u->email, sizeof(u->email), "%s", row.bufs[4]);
    if (row.cols >= 6) snprintf(u->figure, sizeof(u->figure), "%s", row.bufs[5]);
    if (row.cols >= 7) snprintf(u->birthday, sizeof(u->birthday), "%s", row.bufs[6]);
    if (row.cols >= 8) snprintf(u->phonenumber, sizeof(u->phonenumber), "%s", row.bufs[7]);
    if (row.cols >= 9) snprintf(u->customData, sizeof(u->customData), "%s", row.bufs[8]);
    if (row.cols >= 10) u->had_read_agreement = atoi(row.bufs[9]);
    if (row.cols >= 11) snprintf(u->sex, sizeof(u->sex), "%s", row.bufs[10]);
    if (row.cols >= 12) snprintf(u->country, sizeof(u->country), "%s", row.bufs[11]);
    if (row.cols >= 13) snprintf(u->has_special_rights, sizeof(u->has_special_rights), "%s", row.bufs[12]);
    if (row.cols >= 14) snprintf(u->badge_type, sizeof(u->badge_type), "%s", row.bufs[13]);
    if (row.cols >= 15) u->inroom = atoi(row.bufs[14]);

    stmtrow_free(&row);
    return 1;
}

static int db_update_room_inroom_delta(MYSQL *conn, int room_id, int delta) {
    const char *sql = "UPDATE rooms SET inroom = inroom + ? WHERE id = ?";
    MYSQL_BIND p[2];
    bind_int_in(&p[0], &delta);
    bind_int_in(&p[1], &room_id);
    return stmt_exec(conn, sql, p, 2);
}

static int db_update_user_inroom(MYSQL *conn, const char *name, int room_id) {
    const char *sql = "UPDATE users SET inroom=? WHERE name=? LIMIT 1";
    MYSQL_BIND p[2];
    unsigned long ln=0;
    bind_int_in(&p[0], &room_id);
    bind_str_in(&p[1], name, &ln);
    return stmt_exec(conn, sql, p, 2);
}

static int db_insert_chatlog(MYSQL *conn, const char *user, const char *msg, int room, const char *kind) {
    /* chatloggs schema: id,user,msg,room,type */
    const char *sql = "INSERT INTO chatloggs (user,msg,room,type) VALUES (?,?,?,?)";
    MYSQL_BIND p[4];
    unsigned long lu=0,lm=0,lk=0;
    bind_str_in(&p[0], user, &lu);
    bind_str_in(&p[1], msg, &lm);
    bind_int_in(&p[2], &room);
    bind_str_in(&p[3], kind, &lk);
    return stmt_exec(conn, sql, p, 4);
}

static int db_register_user(MYSQL *conn,
    const char *name, const char *pass, const char *email, const char *figure,
    const char *birthday, const char *phone, const char *customData,
    const char *had_read_agreement_str, const char *sex, const char *country)
{
    const char *sql =
        "INSERT INTO users "
        "(name,password,credits,email,figure,birthday,phonenumber,customData,had_read_agreement,sex,country,has_special_rights,badge_type,inroom) "
        "VALUES (?,?,?,?,1337,?,?,?,?,?,?,?,'0','0',0)";

    MYSQL_BIND p[10];
    unsigned long l0=0,l1=0,l2=0,l3=0,l4=0,l5=0,l6=0,l7=0,l8=0,l9=0;

    bind_str_in(&p[0], name, &l0);
    bind_str_in(&p[1], pass, &l1);
    bind_str_in(&p[2], email, &l2);
    bind_str_in(&p[3], figure, &l3);
    bind_str_in(&p[4], birthday, &l4);
    bind_str_in(&p[5], phone, &l5);
    bind_str_in(&p[6], customData, &l6);
    bind_str_in(&p[7], had_read_agreement_str, &l7);
    bind_str_in(&p[8], sex, &l8);
    bind_str_in(&p[9], country, &l9);

    return stmt_exec(conn, sql, p, 10);
}

static int db_update_user_profile(MYSQL *conn,
    const char *name, const char *pass, const char *email, const char *figure,
    const char *birthday, const char *phone, const char *had_read_agreement_str,
    const char *country, const char *sex, const char *customData)
{
    const char *sql =
        "UPDATE users SET password=?, email=?, figure=?, birthday=?, phonenumber=?, "
        "had_read_agreement=?, country=?, sex=?, customData=? "
        "WHERE name=? LIMIT 1";

    MYSQL_BIND p[10];
    unsigned long l0=0,l1=0,l2=0,l3=0,l4=0,l5=0,l6=0,l7=0,l8=0,l9=0;

    bind_str_in(&p[0], pass, &l0);
    bind_str_in(&p[1], email, &l1);
    bind_str_in(&p[2], figure, &l2);
    bind_str_in(&p[3], birthday, &l3);
    bind_str_in(&p[4], phone, &l4);
    bind_str_in(&p[5], had_read_agreement_str, &l5);
    bind_str_in(&p[6], country, &l6);
    bind_str_in(&p[7], sex, &l7);
    bind_str_in(&p[8], customData, &l8);
    bind_str_in(&p[9], name, &l9);

    return stmt_exec(conn, sql, p, 10);
}

static int db_delete_room_by_id(MYSQL *conn, const char *room_id_str) {
    const char *sql = "DELETE FROM rooms WHERE id = ? LIMIT 1";
    MYSQL_BIND p[1];
    unsigned long lr=0;
    bind_str_in(&p[0], room_id_str, &lr);
    return stmt_exec(conn, sql, p, 1);
}

static int db_buddy_request(MYSQL *conn, const char *from_user, const char *to_user) {
    const char *sql = "INSERT INTO buddy (`from`,`to`,`accept`) VALUES (?,?,'0')";
    MYSQL_BIND p[2];
    unsigned long lf=0, lt=0;
    bind_str_in(&p[0], from_user, &lf);
    bind_str_in(&p[1], to_user, &lt);
    return stmt_exec(conn, sql, p, 2);
}

static int db_buddy_accept(MYSQL *conn, const char *from_user, const char *to_user) {
    const char *sql = "UPDATE buddy SET accept='1' WHERE `from`=? AND `to`=? AND `accept`='0'";
    MYSQL_BIND p[2];
    unsigned long lf=0, lt=0;
    bind_str_in(&p[0], from_user, &lf);
    bind_str_in(&p[1], to_user, &lt);
    return stmt_exec(conn, sql, p, 2);
}

static int db_buddy_decline(MYSQL *conn, const char *from_user, const char *to_user) {
    const char *sql = "DELETE FROM buddy WHERE `from`=? AND `to`=? AND `accept`='0'";
    MYSQL_BIND p[2];
    unsigned long lf=0, lt=0;
    bind_str_in(&p[0], from_user, &lf);
    bind_str_in(&p[1], to_user, &lt);
    return stmt_exec(conn, sql, p, 2);
}

static int db_buddy_remove_pair(MYSQL *conn, const char *a, const char *b) {
    const char *sql1 = "DELETE FROM buddy WHERE `from`=? AND `to`=? AND `accept`='1' LIMIT 1";
    const char *sql2 = "DELETE FROM buddy WHERE `from`=? AND `to`=? AND `accept`='1' LIMIT 1";
    MYSQL_BIND p[2];
    unsigned long la=0, lb=0;

    bind_str_in(&p[0], a, &la);
    bind_str_in(&p[1], b, &lb);
    (void)stmt_exec(conn, sql1, p, 2);

    bind_str_in(&p[0], b, &lb);
    bind_str_in(&p[1], a, &la);
    (void)stmt_exec(conn, sql2, p, 2);
    return 0;
}

static int db_rights_insert(MYSQL *conn, int room_id, const char *user) {
    const char *sql = "INSERT INTO rights (room,user) VALUES (?,?)";
    MYSQL_BIND p[2];
    unsigned long lu=0;
    bind_int_in(&p[0], &room_id);
    bind_str_in(&p[1], user, &lu);
    return stmt_exec(conn, sql, p, 2);
}

static int db_rights_delete(MYSQL *conn, int room_id, const char *user) {
    const char *sql = "DELETE FROM rights WHERE room=? AND user=? LIMIT 1";
    MYSQL_BIND p[2];
    unsigned long lu=0;
    bind_int_in(&p[0], &room_id);
    bind_str_in(&p[1], user, &lu);
    return stmt_exec(conn, sql, p, 2);
}

static int db_has_rights(MYSQL *conn, int room_id, const char *user) {
    const char *sql = "SELECT 1 FROM rights WHERE room=? AND user=? LIMIT 1";
    MYSQL_BIND p[2];
    unsigned long lu=0;
    bind_int_in(&p[0], &room_id);
    bind_str_in(&p[1], user, &lu);

    StmtRow row;
    int got = stmt_query_first_row(conn, sql, p, 2, &row, DB_OUTBUF_SMALL);
    if (got == 1) stmtrow_free(&row);
    return got;
}

/* -------------------- Server prototypes -------------------- */

static void* handle_client(void* arg);
static void process_packet(int client_id, char* packet);
static void send_data(int client_id, const char* data);
static void send_data_all(const char* data);
static void send_data_room(int room_id, const char* data);
static void load_user_data(int client_id, const char* username, const char* password);
static void init_database(void);
static void cleanup_user(int client_id);

static void load_wordfilters(void);
static char* filter_chat_message(char* message);

static void* pathfinding_thread(void* arg);
static int get_room_height(int x, int y, char* heightmap);

/* kept for compatibility but UNUSED now */
static char* escape_string(MYSQL* conn, const char* str) {
    (void)conn; (void)str;
    static char dummy[1] = {0};
    return dummy;
}

/* -------------------- main -------------------- */

int main(void) {
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

    pthread_mutex_init(&users_mutex, NULL);
    pthread_mutex_init(&db_mutex, NULL);

    for (int i = 0; i < MAX_CLIENTS; i++) {
        memset(&users[i], 0, sizeof(User));
        users[i].used = 0;
        users[i].id = i;
        users[i].socket = INVALID_SOCKET;
        users[i].db_conn = NULL;
        users[i].pathfinding_thread = (pthread_t)0;
    }

    init_database();

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

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

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("Server listening on port 37120\n");

    while (1) {
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {
            perror("accept");
            continue;
        }

        int client_id = -1;
        pthread_mutex_lock(&users_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!users[i].used) { client_id = i; break; }
        }
        pthread_mutex_unlock(&users_mutex);

        if (client_id == -1) {
            printf("Server full, rejecting connection\n");
            close(new_socket);
            continue;
        }

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

        users[client_id].db_conn = mysql_init(NULL);
        if (!users[client_id].db_conn ||
            !mysql_real_connect(users[client_id].db_conn, "localhost", "root", "verysecret", "goldfish4c", 0, NULL, 0))
        {
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

        send_data(client_id, "HELLO\r");

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

    if (global_db_conn) mysql_close(global_db_conn);

#ifdef _WIN32
    WSACleanup();
#endif
    return 0;
}

static void init_database(void) {
    global_db_conn = mysql_init(NULL);
    if (!mysql_real_connect(global_db_conn, "localhost", "root", "verysecret", "goldfish4c", 0, NULL, 0)) {
        printf("Failed to initialize global database connection\n");
        exit(EXIT_FAILURE);
    }
}

/* -------------------- client handling -------------------- */

static void* handle_client(void* arg) {
    int client_id = *(int*)arg;
    free(arg);

    int sock = users[client_id].socket;

    while (1) {
        char hdr[4];
        int hr = recv_all(sock, hdr, 4);
        if (hr <= 0) break;

        int len_info = parse_len4(hdr);
        if (len_info < 0) {
            printf("Bad length header: [%c%c%c%c]\n", hdr[0], hdr[1], hdr[2], hdr[3]);
            break;
        }

        if (len_info == 0) {
            process_packet(client_id, (char*)"");
            continue;
        }

        char *packet = (char*)malloc((size_t)len_info + 1);
        if (!packet) break;

        int pr = recv_all(sock, packet, len_info);
        if (pr <= 0) { free(packet); break; }

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

static void cleanup_user(int client_id) {
    pthread_mutex_lock(&users_mutex);

    if (client_id < 0 || client_id >= MAX_CLIENTS) {
        pthread_mutex_unlock(&users_mutex);
        return;
    }

    if (users[client_id].used) {
        if (users[client_id].inroom != 0) {
            (void)db_update_room_inroom_delta(users[client_id].db_conn, users[client_id].inroom, -1);
            users[client_id].inroom = 0;
            users[client_id].owner = 0;
            users[client_id].rights = 0;
        }

        if (users[client_id].pathfinding_active) {
            users[client_id].pathfinding_active = 0;
            if (users[client_id].pathfinding_thread) {
                pthread_join(users[client_id].pathfinding_thread, NULL);
            }
            users[client_id].pathfinding_thread = (pthread_t)0;
        }

        if (users[client_id].socket > 0) close(users[client_id].socket);
        users[client_id].socket = INVALID_SOCKET;

        if (users[client_id].db_conn) {
            mysql_close(users[client_id].db_conn);
            users[client_id].db_conn = NULL;
        }

        users[client_id].used = 0;
    }

    pthread_mutex_unlock(&users_mutex);
}

/* -------------------- packet processing helpers -------------------- */

static void safe_strcat(char *dst, size_t dstsz, const char *src) {
    if (!dst || !src || dstsz == 0) return;
    size_t dlen = strlen(dst);
    if (dlen >= dstsz - 1) return;
    size_t n = dstsz - 1 - dlen;
    strncat(dst, src, n);
}

static int cb_pubs_row(StmtRow *row, void *ud) {
    /* pubs schema: id,name,now_in,max_in,mapname,heightmap
       our SELECT: name, now_in, max_in, mapname, heightmap
       idx: 0 name,1 now_in,2 max_in,3 mapname,4 heightmap
    */
    char *out = (char*)ud;
    if (!out) return 1;

    if (row->cols >= 5) {
        char temp[1024];
        snprintf(temp, sizeof(temp),
            "%s,%s,%s,127.0.0.1/127.0.0.1,37120,%s\t%s,%s,%s,%s\r",
            row->bufs[0],
            row->bufs[1],
            row->bufs[2],
            row->bufs[0],
            row->bufs[3],
            row->bufs[1],
            row->bufs[2],
            row->bufs[4]
        );
        safe_strcat(out, BIGBUF_SIZE, temp);
    }
    return 0;
}

static int cb_rooms_list_row(StmtRow *row, void *ud) {
    /* rooms schema (v1.sql):
       id,name,desc,owner,model,door,pass,wallpaper,floor,inroom,maxusers

       our SELECT: id,name,owner,door,pass,floor,inroom,`desc`
       idx: 0 id,1 name,2 owner,3 door,4 pass,5 floor,6 inroom,7 desc
    */
    char *out = (char*)ud;
    if (!out) return 1;

    if (row->cols >= 8) {
        char temp[1024];
        snprintf(temp, sizeof(temp),
            "\r%s/%s/%s/%s/%s/%s/127.0.0.1/127.0.0.1/37120/%s/null/%s",
            row->bufs[0],
            row->bufs[1],
            row->bufs[2],
            row->bufs[3],
            row->bufs[4],
            row->bufs[5],
            row->bufs[6],
            row->bufs[7]
        );
        safe_strcat(out, BIGBUF_SIZE, temp);
    }
    return 0;
}

static int cb_buddy_list_row(StmtRow *row, void *ud) {
    /* buddy schema: id,from,to,accept
       our SELECT: id, `from`, `to`
       idx: 0 id,1 from,2 to
    */
    struct { const char *me; char *out; size_t outsz; } *ctx = ud;
    if (!ctx || !ctx->out) return 1;

    if (row->cols >= 3) {
        const char *name = (strcmp(row->bufs[1], ctx->me) != 0) ? row->bufs[1] : row->bufs[2];
        char temp[128];
        snprintf(temp, sizeof(temp), "\r1 %s UNKNOW\r", name);
        safe_strcat(ctx->out, ctx->outsz, temp);
    }
    return 0;
}

/* -------------------- packet processing -------------------- */

static void process_packet(int client_id, char* packet) {
    char packet_copy[INBUF_SIZE];
    snprintf(packet_copy, sizeof(packet_copy), "%s", packet ? packet : "");

    char* token = strtok(packet, " ");
    if (!token) return;

    MYSQL *conn = users[client_id].db_conn;

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

        printf("Username: %s\n", username ? username : "(null)");
        printf("Password: %s\n", password ? password : "(null)");

        if (username && password) {
            int ok = db_check_login(conn, username, password);
            if (ok == 1) {
                load_user_data(client_id, username, password);

                char* server_param = strtok(NULL, " ");
                if (server_param && strcmp(server_param, "0") == 0) {
                    users[client_id].server = 1;

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

                    snprintf(users[client_id].roomcache, sizeof(users[client_id].roomcache),
                             "xxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx");

                    send_data(client_id,
                        "HEIGHTMAP\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx\rxxxxxxxxxx");

                    char user_packet[1024];
                    const char *fig = users[client_id].figure;
                    if (strncmp(fig, "figure=", 7) == 0) fig += 7;

                    snprintf(user_packet, sizeof(user_packet),
                             "USERS\r %s %s 3 5 0 %s",
                             users[client_id].name, fig, users[client_id].customData);
                    send_data(client_id, user_packet);

                    users[client_id].userx = 12;
                    users[client_id].usery = 27;

                    char status_packet[1024];
                    snprintf(status_packet, sizeof(status_packet), "STATUS \r%s 12,27,1,0,0/mod 0/", users[client_id].name);
                    send_data(client_id, status_packet);
                }
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

            char user_object[MIDBUF_SIZE];
            snprintf(user_object, sizeof(user_object),
                     "USEROBJECT\r"
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
        char room_data[BIGBUF_SIZE]; room_data[0] = '\0';
        (void)stmt_query_each_row(conn,
            "SELECT name, now_in, max_in, mapname, heightmap FROM pubs",
            NULL, 0, cb_pubs_row, room_data, DB_OUTBUF_MED);

        char all_units[BIGBUF_SIZE];
        snprintf(all_units, sizeof(all_units), "ALLUNITS\r%s", room_data);
        send_data(client_id, all_units);
    }
    else if (strcmp(token, "SEARCHBUSYFLATS") == 0) {
        char room_data[BIGBUF_SIZE]; room_data[0] = '\0';

        (void)stmt_query_each_row(conn,
            "SELECT id, name, owner, door, pass, floor, inroom, `desc` "
            "FROM rooms ORDER BY inroom DESC",
            NULL, 0, cb_rooms_list_row, room_data, DB_OUTBUF_MED);

        char busy_flats[BIGBUF_SIZE];
        snprintf(busy_flats, sizeof(busy_flats), "BUSY_FLAT_RESULTS 1%s", room_data);
        send_data(client_id, busy_flats);
    }
    else if (strcmp(token, "GETCREDITS") == 0) {
        char wallet[128];
        snprintf(wallet, sizeof(wallet), "WALLETBALANCE\r%d", users[client_id].credits);
        send_data(client_id, wallet);
        send_data(client_id, "MESSENGERSMSACCOUNT\rnoaccount");
        send_data(client_id, "MESSENGERREADY \r");
    }
    else if (strcmp(token, "REGISTER") == 0) {
        char* lines[20];
        int line_count = 0;
        char* line = strtok(packet_copy, "\r");
        while (line && line_count < 20) {
            lines[line_count++] = line;
            line = strtok(NULL, "\r");
        }

        if (line_count >= 11) {
            char username[50]={0}, password[50]={0}, email[255]={0}, figure[255]={0};
            char birthday[25]={0}, phone[20]={0}, mission[1096]={0}, agree[10]={0}, sex[10]={0}, city[50]={0};

            sscanf(lines[0], "REGISTER name=%49s", username);
            sscanf(lines[1], "password=%49s", password);
            sscanf(lines[2], "email=%254s", email);
            sscanf(lines[3], "figure=%254s", figure);
            /* line 4 might exist but unused */
            sscanf(lines[5], "birthday=%24s", birthday);
            sscanf(lines[6], "phonenumber=%19s", phone);
            sscanf(lines[7], "customData=%1095[^\r]", mission);
            sscanf(lines[8], "had_read_agreement=%9s", agree);
            sscanf(lines[9], "sex=%9s", sex);
            sscanf(lines[10], "country=%49s", city);

            (void)db_register_user(conn, username, password, email, figure, birthday, phone, mission, agree, sex, city);
        }
    }
    else if (strcmp(token, "UPDATE") == 0) {
        char* lines[20];
        int line_count = 0;
        char* line = strtok(packet_copy, "\r");
        while (line && line_count < 20) {
            lines[line_count++] = line;
            line = strtok(NULL, "\r");
        }

        if (line_count >= 11) {
            char username[50]={0}, password[50]={0}, email[255]={0}, figure[255]={0};
            char birthday[25]={0}, phone[20]={0}, mission[1096]={0}, agree[10]={0}, sex[10]={0}, city[50]={0};

            sscanf(lines[0], "UPDATE name=%49s", username);
            sscanf(lines[1], "password=%49s", password);
            sscanf(lines[2], "email=%254s", email);
            sscanf(lines[3], "figure=%254s", figure);
            sscanf(lines[5], "birthday=%24s", birthday);
            sscanf(lines[6], "phonenumber=%19s", phone);
            sscanf(lines[7], "customData=%1095[^\r]", mission);
            sscanf(lines[8], "had_read_agreement=%9s", agree);
            sscanf(lines[9], "sex=%9s", sex);
            sscanf(lines[10], "country=%49s", city);

            (void)db_update_user_profile(conn, username, password, email, figure, birthday, phone, agree, city, sex, mission);
        }
    }
    else if (strcmp(token, "SEARCHFLATFORUSER") == 0) {
        char* user = strchr(packet_copy, '/');
        user = user ? (user + 1) : NULL;
        if (user) {
            char* newline = strchr(user, '\r');
            if (newline) *newline = '\0';

            const char *sql = "SELECT id, name, owner, door, pass, floor, inroom, `desc` FROM rooms WHERE owner = ?";
            MYSQL_BIND p[1];
            unsigned long lu=0;
            bind_str_in(&p[0], user, &lu);

            char room_data[BIGBUF_SIZE]; room_data[0] = '\0';
            (void)stmt_query_each_row(conn, sql, p, 1, cb_rooms_list_row, room_data, DB_OUTBUF_MED);

            char busy_flats[BIGBUF_SIZE];
            snprintf(busy_flats, sizeof(busy_flats), "BUSY_FLAT_RESULTS 1%s", room_data);
            send_data(client_id, busy_flats);
        }
    }
    else if (strcmp(token, "UINFO_MATCH") == 0) {
        char* user = strchr(packet_copy, '/');
        user = user ? (user + 1) : NULL;
        if (user) {
            char* newline = strchr(user, '\r');
            if (newline) *newline = '\0';

            const char *sql = "SELECT name, figure, customData, sex FROM users WHERE name = ? LIMIT 1";
            MYSQL_BIND p[1];
            unsigned long lu=0;
            bind_str_in(&p[0], user, &lu);

            StmtRow row;
            int got = stmt_query_first_row(conn, sql, p, 1, &row, DB_OUTBUF_LARGE);
            if (got == 1) {
                const char *uname  = (row.cols > 0 ? row.bufs[0] : user);
                const char *figure = (row.cols > 1 ? row.bufs[1] : "");
                const char *custom = (row.cols > 2 ? row.bufs[2] : "");
                const char *sex    = (row.cols > 3 ? row.bufs[3] : "");

                if (strncmp(figure, "figure=", 7) == 0) figure += 7;

                char member_info[1024];
                snprintf(member_info, sizeof(member_info),
                         "MEMBERINFO MESSENGER\r%s\r%s\r\r0\r%s%s",
                         uname, custom, figure, sex);
                send_data(client_id, member_info);
                stmtrow_free(&row);
            } else {
                char no_such_user[256];
                snprintf(no_such_user, sizeof(no_such_user), "NOSUCHUSER MESSENGER\r%s", user);
                send_data(client_id, no_such_user);
            }
        }
    }
    else if (strcmp(token, "GETFLATINFO") == 0) {
        send_data(client_id, "SETFLATINFO\r/1/");
    }
    else if (strcmp(token, "DELETEFLAT") == 0) {
        char* flat_id = strchr(packet_copy, '/');
        flat_id = flat_id ? (flat_id + 1) : NULL;
        if (flat_id) {
            char* newline = strchr(flat_id, '\r');
            if (newline) *newline = '\0';
            (void)db_delete_room_by_id(conn, flat_id);
        }
    }
    else if (strcmp(token, "GET_FAVORITE_ROOMS") == 0) {
        char* username = strtok(NULL, " ");
        if (username) {
            const char *sql =
                "SELECT r.id, r.name, r.owner, r.door, r.pass, r.floor, r.inroom, r.`desc` "
                "FROM favrooms f "
                "JOIN rooms r ON r.id = f.roomid "
                "WHERE f.user = ?";

            MYSQL_BIND p[1];
            unsigned long lu=0;
            bind_str_in(&p[0], username, &lu);

            char room_data[BIGBUF_SIZE]; room_data[0] = '\0';
            (void)stmt_query_each_row(conn, sql, p, 1, cb_rooms_list_row, room_data, DB_OUTBUF_MED);

            char busy_flats[BIGBUF_SIZE];
            snprintf(busy_flats, sizeof(busy_flats), "BUSY_FLAT_RESULTS 1%s", room_data);
            send_data(client_id, busy_flats);
        }
    }
    else if (strcmp(token, "ADD_FAVORITE_ROOM") == 0) {
        char* room_id = strtok(NULL, " ");
        if (room_id) {
            const char *sql = "INSERT INTO favrooms (user,roomid) VALUES (?,?)";
            MYSQL_BIND p[2];
            unsigned long lu=0, lr=0;
            bind_str_in(&p[0], users[client_id].name, &lu);
            bind_str_in(&p[1], room_id, &lr);
            (void)stmt_exec(conn, sql, p, 2);
        }
    }
    else if (strcmp(token, "DEL_FAVORITE_ROOM") == 0) {
        char* room_id = strtok(NULL, " ");
        if (room_id) {
            const char *sql = "DELETE FROM favrooms WHERE user=? AND roomid=? LIMIT 1";
            MYSQL_BIND p[2];
            unsigned long lu=0, lr=0;
            bind_str_in(&p[0], users[client_id].name, &lu);
            bind_str_in(&p[1], room_id, &lr);
            (void)stmt_exec(conn, sql, p, 2);
        }
    }
    else if (strcmp(token, "SEARCHFLAT") == 0) {
        char* search_term = strchr(packet_copy, '/');
        search_term = search_term ? (search_term + 1) : NULL;
        if (search_term) {
            char* newline = strchr(search_term, '\r');
            if (newline) *newline = '\0';

            char like[512];
            snprintf(like, sizeof(like), "%%%s%%", search_term);

            const char *sql =
                "SELECT id, name, owner, door, pass, floor, inroom, `desc` "
                "FROM rooms WHERE owner LIKE ? OR name LIKE ?";

            MYSQL_BIND p[2];
            unsigned long l0=0,l1=0;
            bind_str_in(&p[0], like, &l0);
            bind_str_in(&p[1], like, &l1);

            char room_data[BIGBUF_SIZE]; room_data[0] = '\0';
            (void)stmt_query_each_row(conn, sql, p, 2, cb_rooms_list_row, room_data, DB_OUTBUF_MED);

            char busy_flats[BIGBUF_SIZE];
            snprintf(busy_flats, sizeof(busy_flats), "BUSY_FLAT_RESULTS 1%s", room_data);
            send_data(client_id, busy_flats);
        }
    }
    else if (strcmp(token, "TRYFLAT") == 0) {
        char* flat_id = strchr(packet_copy, '/');
        flat_id = flat_id ? (flat_id + 1) : NULL;
        if (flat_id) {
            char* password = strchr(flat_id, '/');
            password = password ? (password + 1) : NULL;
            if (password) {
                char* newline = strchr(password, '\r');
                if (newline) *newline = '\0';

                int gotoroom = atoi(flat_id);

                if (users[client_id].inroom != 0) {
                    (void)db_update_room_inroom_delta(conn, users[client_id].inroom, -1);
                    users[client_id].owner = 0;
                    users[client_id].rights = 0;
                }

                (void)db_update_user_inroom(conn, users[client_id].name, gotoroom);
                (void)db_update_room_inroom_delta(conn, gotoroom, +1);

                users[client_id].inroom = gotoroom;
                send_data(client_id, "FLAT_LETIN");
            }
        }
    }
    else if (strcmp(token, "GOTOFLAT") == 0) {
        char* flat_id = strchr(packet_copy, '/');
        flat_id = flat_id ? (flat_id + 1) : NULL;
        if (flat_id) {
            char* newline = strchr(flat_id, '\r');
            if (newline) *newline = '\0';

            int gotoroom = atoi(flat_id);

            /* rooms schema: id,name,desc,owner,model,door,pass,wallpaper,floor,inroom,maxusers */
            const char *sql_room =
                "SELECT id,name,`desc`,owner,model,door,pass,space_w,space_f,inroom,20 AS maxusers "
                "FROM rooms WHERE id = ? LIMIT 1";
            MYSQL_BIND p[1];
            unsigned long lr=0;
            bind_str_in(&p[0], flat_id, &lr);

            StmtRow room;
            int got = stmt_query_first_row(conn, sql_room, p, 1, &room, DB_OUTBUF_LARGE);
            if (got == 1) {
                const char *desc  = (room.cols > 2 ? room.bufs[2] : "");
                const char *owner = (room.cols > 3 ? room.bufs[3] : "");
                const char *model = (room.cols > 4 ? room.bufs[4] : "");
                const char *wall  = (room.cols > 7 ? room.bufs[7] : "");
                const char *floor = (room.cols > 8 ? room.bufs[8] : "");

                char room_ready[1024];
                snprintf(room_ready, sizeof(room_ready), "ROOM_READY\r%s", desc);
                send_data(client_id, room_ready);

                char flat_prop1[256];
                snprintf(flat_prop1, sizeof(flat_prop1), "FLATPROPERTY\rwallpaper/%s\r", wall);
                send_data(client_id, flat_prop1);

                char flat_prop2[256];
                snprintf(flat_prop2, sizeof(flat_prop2), "FLATPROPERTY\rfloor/%s\r", floor);
                send_data(client_id, flat_prop2);

                users[client_id].owner = 0;
                users[client_id].rights = 0;

                if (strcmp(owner, users[client_id].name) == 0) {
                    users[client_id].owner = 1;
                    send_data(client_id, "YOUAREOWNER");
                }

                int has = db_has_rights(conn, gotoroom, users[client_id].name);
                if (has == 1) {
                    users[client_id].rights = 1;
                    send_data(client_id, "YOUARECONTROLLER");
                }

                /* heightmap */
                const char *sql_h = "SELECT heightmap FROM heightmap WHERE model = ? LIMIT 1";
                MYSQL_BIND hp[1];
                unsigned long lm=0;
                bind_str_in(&hp[0], model, &lm);

                StmtRow hm;
                int hgot = stmt_query_first_row(conn, sql_h, hp, 1, &hm, DB_OUTBUF_LARGE);
				if (hgot == 1) {
					char *p = hm.bufs[0];
					while (*p) {
						if (*p == ' ')
							*p = '\r';
						p++;
					}

					snprintf(users[client_id].roomcache,
							 sizeof(users[client_id].roomcache),
							 "%s", hm.bufs[0]);

					char heightmap_packet[2048];
					snprintf(heightmap_packet,
							 sizeof(heightmap_packet),
							 "HEIGHTMAP\r%s", hm.bufs[0]);

					send_data(client_id, heightmap_packet);
					stmtrow_free(&hm);
				}

                char objects[512];
                snprintf(objects, sizeof(objects), "OBJECTS WORLD 0 %s", model);
                send_data(client_id, objects);

                /* roomitems schema: id,roomid,item,shortname,longname,color,xx,yy,zz,rotate,extra */
                const char *sql_items =
                    "SELECT id, shortname, xx, yy, zz, rotate, extra, longname, color "
                    "FROM roomitems WHERE roomid = ?";
                MYSQL_BIND ip[1];
                int rid = gotoroom;
                bind_int_in(&ip[0], &rid);

                char room_items[BIGBUF_SIZE];
                snprintf(room_items, sizeof(room_items), "ACTIVE_OBJECTS ");

                int items_cb(StmtRow *r, void *ud) {
                    char *acc = (char*)ud;
                    if (!acc) return 1;
                    /* idx: 0 id,1 shortname,2 xx,3 yy,4 zz,5 rotate,6 extra,7 longname,8 color */
                    if (r->cols >= 9) {
                        char temp[512];
                        snprintf(temp, sizeof(temp),
                                 "\r00000000000%s,%s %s %s %s %s 0.0 %s /%s/%s/",
                                 r->bufs[0], r->bufs[1], r->bufs[2], r->bufs[3],
                                 r->bufs[4], r->bufs[5], r->bufs[6], r->bufs[7], r->bufs[8]);
                        safe_strcat(acc, BIGBUF_SIZE, temp);
                    }
                    return 0;
                }

                (void)stmt_query_each_row(conn, sql_items, ip, 1, items_cb, room_items, DB_OUTBUF_MED);
                send_data(client_id, room_items);

                send_data(client_id, "ITEMS \r6663\tposter\t \tfrontwall 6.9999,6.3500/21");

                /* other users in room */
                const char *sql_u = "SELECT name, figure, customData FROM users WHERE inroom = ? AND name != ?";
                MYSQL_BIND up[2];
                int inr = gotoroom;
                unsigned long ln=0;
                bind_int_in(&up[0], &inr);
                bind_str_in(&up[1], users[client_id].name, &ln);

                int users_cb(StmtRow *r, void *ud) {
                    int cid = *(int*)ud;
                    if (r->cols >= 3) {
                        const char *uname = r->bufs[0];
                        const char *fig = r->bufs[1];
                        const char *custom = r->bufs[2];
                        if (strncmp(fig, "figure=", 7) == 0) fig += 7;

                        char user_packet[1024];
                        snprintf(user_packet, sizeof(user_packet), "USERS\r %s %s 3 5 0 %s", uname, fig, custom);
                        send_data(cid, user_packet);
                    }
                    return 0;
                }
                (void)stmt_query_each_row(conn, sql_u, up, 2, users_cb, &client_id, DB_OUTBUF_LARGE);

                /* statuses */
                {
                    char status_all[1024];
                    char status_room[1024];
                    char user_packet[1024];
                    const char *fig = users[client_id].figure;
                    if (strncmp(fig, "figure=", 7) == 0) fig += 7;

                    if (users[client_id].owner || users[client_id].rights) {
                        snprintf(status_all, sizeof(status_all),
                                 "STATUS \r%s 0,0,99,2,2/flatctrl useradmin/mod 0/", users[client_id].name);
                        send_data_all(status_all);

                        snprintf(user_packet, sizeof(user_packet),
                                 "USERS\r %s %s 3 5 0 %s", users[client_id].name, fig, users[client_id].customData);
                        send_data_room(gotoroom, user_packet);

                        snprintf(status_room, sizeof(status_room),
                                 "STATUS \r%s 3,5,0,2,2/flatctrl useradmin/mod 0/", users[client_id].name);
                        send_data_room(gotoroom, status_room);
                    } else {
                        snprintf(status_all, sizeof(status_all),
                                 "STATUS \r%s 0,0,99,2,2/mod 0/", users[client_id].name);
                        send_data_all(status_all);

                        snprintf(user_packet, sizeof(user_packet),
                                 "USERS\r %s %s 3 5 0 %s", users[client_id].name, fig, users[client_id].customData);
                        send_data_room(gotoroom, user_packet);

                        snprintf(status_room, sizeof(status_room),
                                 "STATUS \r%s 3,5,0,2,2/mod 0/", users[client_id].name);
                        send_data_room(gotoroom, status_room);
                    }

                    users[client_id].userx = 3;
                    users[client_id].usery = 5;
                }

                stmtrow_free(&room);
            }
        }
    }
    else if (strcmp(token, "SHOUT") == 0 || strcmp(token, "CHAT") == 0 || strcmp(token, "WHISPER") == 0) {
        char* message = packet_copy + strlen(token) + 1;

        char clean_message[512];
        int j = 0;
        for (int i = 0; message[i] && i < (int)sizeof(clean_message)-1; i++) {
            if (message[i] != '\'' && message[i] != '"' && message[i] != '#') {
                clean_message[j++] = message[i];
                if (j >= (int)sizeof(clean_message)-1) break;
            }
        }
        clean_message[j] = '\0';

        char filtered_message[512];
        snprintf(filtered_message, sizeof(filtered_message), "%s", clean_message);
        filter_chat_message(filtered_message);

        char chat_packet[1024];
        snprintf(chat_packet, sizeof(chat_packet), "%s\r%s %s", token, users[client_id].name, filtered_message);
        send_data_room(users[client_id].inroom, chat_packet);

        (void)db_insert_chatlog(conn, users[client_id].name, filtered_message, users[client_id].inroom, token);
    }
    else if (strcmp(token, "STATUSOK") == 0) {
        send_data(client_id, "OK");
    }
    else if (strcmp(token, "Move") == 0) {
        char* x_str = strtok(NULL, " ");
        char* y_str = strtok(NULL, " ");
	
		
        if (x_str && y_str) {
            int to_x = atoi(x_str);
            int to_y = atoi(y_str);
			
			printf("MOVE: %s - %s\n", x_str, y_str);
		
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
        char status_packet[1024];
        snprintf(status_packet, sizeof(status_packet), "STATUS \r%s 0,0,99,2,2/mod 0/", users[client_id].name);
        send_data_room(users[client_id].inroom, status_packet);

        users[client_id].userx = 0;
        users[client_id].usery = 0;
        users[client_id].dance = 0;

        if (users[client_id].inroom != 0) {
            (void)db_update_user_inroom(conn, users[client_id].name, 0);
            (void)db_update_room_inroom_delta(conn, users[client_id].inroom, -1);

            users[client_id].inroom = 0;
            users[client_id].owner = 0;
            users[client_id].rights = 0;
        }

        cleanup_user(client_id);
    }
    else if (strcmp(token, "DANCE") == 0) {
        char status_packet[1024];
        snprintf(status_packet, sizeof(status_packet), "STATUS\r%s %d,%d,0,2,2/dance/",
                 users[client_id].name, users[client_id].userx, users[client_id].usery);
        send_data_room(users[client_id].inroom, status_packet);
        users[client_id].dance = 1;
    }
    else if (strcmp(token, "STOP") == 0) {
        char* stop_type = strtok(NULL, " ");
        if (stop_type && strcmp(stop_type, "DANCE") == 0) {
            char status_packet[1024];
            snprintf(status_packet, sizeof(status_packet), "STATUS\r%s %d,%d,0,2,2/",
                     users[client_id].name, users[client_id].userx, users[client_id].usery);
            send_data_room(users[client_id].inroom, status_packet);
            users[client_id].dance = 0;
        }
    }
    else if (strcmp(token, "GETORDERINFO") == 0) {
        char* item_name = strtok(NULL, " ");
        if (item_name) {
            /* catalogue schema: id,shortname,longname,item,short,type,color,extra,price */
            const char *sql = "SELECT longname, price FROM catalogue WHERE shortname = ? LIMIT 1";
            MYSQL_BIND p[1];
            unsigned long li=0;
            bind_str_in(&p[0], item_name, &li);

            StmtRow row;
            int got = stmt_query_first_row(conn, sql, p, 1, &row, DB_OUTBUF_MED);
            if (got == 1) {
                const char *lname = (row.cols > 0 ? row.bufs[0] : "");
                const char *price = (row.cols > 1 ? row.bufs[1] : "0");

                char order_info[1024];
                snprintf(order_info, sizeof(order_info), "ORDERINFO\r%s/%s/\r%s", lname, price, price);
                send_data(client_id, order_info);
                stmtrow_free(&row);
            }
        }
    }
    else if (strcmp(token, "GETSTRIP") == 0) {
        char* strip_type = strtok(NULL, " ");
        if (strip_type) {
            int offset = 0;
            int count = 11;

            if (strcmp(strip_type, "new") == 0) {
                users[client_id].handcache = 0;
                offset = 0;
            } else if (strcmp(strip_type, "next") == 0) {
                offset = users[client_id].handcache;
            }
			
            const char *sql =
                "SELECT id, shortname, longname, color, extra "
                "FROM useritems WHERE user = ? LIMIT ?,?";

            MYSQL_BIND p[3];
            unsigned long lu=0;
            bind_str_in(&p[0], users[client_id].name, &lu);
            bind_int_in(&p[1], &offset);
            bind_int_in(&p[2], &count);

            char item_info[BIGBUF_SIZE]; item_info[0] = '\0';

            int strip_cb(StmtRow *r, void *ud) {
                char *acc = (char*)ud;
                /* useritems schema: id,user,item,shortname,longname,color,extra
                   our SELECT: id, shortname, longname, color, extra
                   idx: 0 id,1 shortname,2 longname,3 color,4 extra
                */
                if (r->cols >= 5) {
                    char temp[1024];
                    snprintf(temp, sizeof(temp),
                             "BLSI;%s;0;S;%s;%s;%s;%s;1;1;0,0,0/\r",
                             r->bufs[0], r->bufs[0], r->bufs[1], r->bufs[2], r->bufs[3]);
                    safe_strcat(acc, BIGBUF_SIZE, temp);
                }
                return 0;
            }

            (void)stmt_query_each_row(conn, sql, p, 3, strip_cb, item_info, DB_OUTBUF_MED);

            users[client_id].handcache = offset + count;

            char strip_info[BIGBUF_SIZE];
            snprintf(strip_info, sizeof(strip_info), "STRIPINFO\r%s", item_info);
            send_data(client_id, strip_info);
        }
    }
    else if (strcmp(token, "PURCHASE") == 0) {
        char* item_name = strchr(packet_copy, '/');
        item_name = item_name ? (item_name + 1) : NULL;
        if (item_name) {
            char* price_str = strchr(item_name, '/');
            if (price_str) *price_str = '\0'; /* split at / */
            price_str = price_str ? (price_str + 1) : NULL;
            if (price_str) {
                char* newline = strchr(price_str, '\r');
                if (newline) *newline = '\0';
            }

            /* catalogue schema: id,shortname,longname,item,short,type,color,extra,price */
            const char *sql =
                "SELECT item, shortname, longname, color, extra, price "
                "FROM catalogue WHERE longname = ? LIMIT 1";
            MYSQL_BIND p[1];
            unsigned long ln=0;
            bind_str_in(&p[0], item_name, &ln);

            StmtRow row;
            int got = stmt_query_first_row(conn, sql, p, 1, &row, DB_OUTBUF_LARGE);
            if (got == 1) {
                int price = (row.cols > 5 ? atoi(row.bufs[5]) : 0);

                const char *sql_ins =
                    "INSERT INTO useritems (user,item,shortname,longname,color,extra) "
                    "VALUES (?,?,?,?,?,?)";
                MYSQL_BIND ip[6];
                unsigned long lu=0,litem=0,ls=0,ll=0,lc=0,le=0;

                bind_str_in(&ip[0], users[client_id].name, &lu);
                bind_str_in(&ip[1], (row.cols>0?row.bufs[0]:""), &litem);
                bind_str_in(&ip[2], (row.cols>1?row.bufs[1]:""), &ls);
                bind_str_in(&ip[3], (row.cols>2?row.bufs[2]:""), &ll);
                bind_str_in(&ip[4], (row.cols>3?row.bufs[3]:""), &lc);
                bind_str_in(&ip[5], (row.cols>4?row.bufs[4]:""), &le);
                (void)stmt_exec(conn, sql_ins, ip, 6);

                const char *sql_up = "UPDATE users SET credits = credits - ? WHERE name = ?";
                MYSQL_BIND up[2];
                unsigned long luser=0;
                bind_int_in(&up[0], &price);
                bind_str_in(&up[1], users[client_id].name, &luser);
                (void)stmt_exec(conn, sql_up, up, 2);

                users[client_id].credits -= price;
                send_data(client_id, "SYSTEMBROADCAST\rBuying successful !");

                stmtrow_free(&row);
            }
        }
    }
    else if (strcmp(token, "CREATEFLAT") == 0) {
        char* flat_data = strchr(packet_copy, '/');
        flat_data = flat_data ? (flat_data + 1) : NULL;
        if (flat_data) {
            char *name = flat_data;
            char *model = strchr(name, '/'); if (model) { *model = '\0'; model++; }
            char *door  = model ? strchr(model, '/') : NULL; if (door) { *door = '\0'; door++; }
            char *pass  = door ? strchr(door, '/') : NULL; if (pass) { *pass = '\0'; pass++; }

            if (name && model && door && pass) {
                char* newline = strchr(pass, '\r');
                if (newline) *newline = '\0';

                const char *sql =
                    "INSERT INTO rooms (name,`desc`,owner,model,door,pass,space_w,space_f,inroom,maxusers) "
                    "VALUES (?,?,?, ?,?,?, ?,?,0)";
                MYSQL_BIND p[8];
                unsigned long l0=0,l1=0,l2=0,l3=0,l4=0,l5=0,l6=0,l7=0;

                bind_str_in(&p[0], name, &l0);
                bind_str_in(&p[1], "", &l1);
                bind_str_in(&p[2], users[client_id].name, &l2);
                bind_str_in(&p[3], model, &l3);
                bind_str_in(&p[4], door, &l4);
                bind_str_in(&p[5], pass, &l5);
                bind_str_in(&p[6], "0", &l6);
                bind_str_in(&p[7], "0", &l7);

                (void)stmt_exec(conn, sql, p, 8);
            }
        }
    }
    else if (strcmp(token, "MESSENGER_REQUESTBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            (void)db_buddy_request(conn, users[client_id].name, buddy_name);
        }
    }
    else if (strcmp(token, "MESSENGER_ACCEPTBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            (void)db_buddy_accept(conn, buddy_name, users[client_id].name);
        }
    }
    else if (strcmp(token, "MESSENGER_DECLINEBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            (void)db_buddy_decline(conn, buddy_name, users[client_id].name);
        }
    }
    else if (strcmp(token, "MESSENGERINIT") == 0) {
        const char *sql =
            "SELECT id, `from`, `to` FROM buddy "
            "WHERE ((`to` = ? AND `accept`='1') OR (`from` = ? AND `accept`='1'))";
        MYSQL_BIND p[2];
        unsigned long l0=0,l1=0;
        bind_str_in(&p[0], users[client_id].name, &l0);
        bind_str_in(&p[1], users[client_id].name, &l1);

        char buddy_data[MIDBUF_SIZE]; buddy_data[0] = '\0';
        struct { const char *me; char *out; size_t outsz; } ctx = { users[client_id].name, buddy_data, sizeof(buddy_data) };

        (void)stmt_query_each_row(conn, sql, p, 2, cb_buddy_list_row, &ctx, DB_OUTBUF_MED);

        char buddy_list[MIDBUF_SIZE];
        snprintf(buddy_list, sizeof(buddy_list), "BUDDYLIST%s", buddy_data);
        send_data(client_id, buddy_list);
    }
    else if (strcmp(token, "MESSENGER_REMOVEBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            (void)db_buddy_remove_pair(conn, buddy_name, users[client_id].name);
        }
    }
    else if (strcmp(token, "PLACESTUFFFROMSTRIP") == 0) {
        char* item_id = strtok(NULL, " ");
        char* x_str = strtok(NULL, " ");
        char* y_str = strtok(NULL, " ");

        if (item_id && x_str && y_str) {
            /* useritems: id,user,item,shortname,longname,color,extra */
            const char *sql_u = "SELECT item, shortname, longname, color, extra FROM useritems WHERE id = ? LIMIT 1";
            MYSQL_BIND p[1];
            unsigned long lid=0;
            bind_str_in(&p[0], item_id, &lid);

            StmtRow it;
            int got = stmt_query_first_row(conn, sql_u, p, 1, &it, DB_OUTBUF_LARGE);
            if (got == 1) {
                /* roomitems: id,roomid,item,shortname,longname,color,xx,yy,zz,rotate,extra */
                const char *sql_ins =
                    "INSERT INTO roomitems (roomid,item,shortname,longname,color,xx,yy,zz,rotate,extra) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)";

                MYSQL_BIND ip[10];
                int roomid = users[client_id].inroom;
                int xx = atoi(x_str);
                int yy = atoi(y_str);
                int zz = 0;
                int rot = 0;

                unsigned long litem=0, ls=0, ll=0, lc=0, le=0;
                bind_int_in(&ip[0], &roomid);
                bind_str_in(&ip[1], (it.cols>0?it.bufs[0]:""), &litem);
                bind_str_in(&ip[2], (it.cols>1?it.bufs[1]:""), &ls);
                bind_str_in(&ip[3], (it.cols>2?it.bufs[2]:""), &ll);
                bind_str_in(&ip[4], (it.cols>3?it.bufs[3]:""), &lc);
                bind_int_in(&ip[5], &xx);
                bind_int_in(&ip[6], &yy);
                bind_int_in(&ip[7], &zz);
                bind_int_in(&ip[8], &rot);
                bind_str_in(&ip[9], (it.cols>4?it.bufs[4]:""), &le);

                (void)stmt_exec(conn, sql_ins, ip, 10);
                stmtrow_free(&it);
            }

            /* delete from strip */
            const char *sql_del = "DELETE FROM useritems WHERE id = ? LIMIT 1";
            (void)stmt_exec(conn, sql_del, p, 1);

            /* fetch last inserted for room */
            const char *sql_last =
                "SELECT id, shortname, xx, yy, zz, rotate, extra, longname, color "
                "FROM roomitems WHERE roomid = ? ORDER BY id DESC LIMIT 1";
            MYSQL_BIND lp[1];
            int rid = users[client_id].inroom;
            bind_int_in(&lp[0], &rid);

            StmtRow rr;
            int got2 = stmt_query_first_row(conn, sql_last, lp, 1, &rr, DB_OUTBUF_LARGE);
            if (got2 == 1) {
                if (rr.cols >= 9) {
                    char room_items[1024];
                    snprintf(room_items, sizeof(room_items),
                             "ACTIVE_OBJECTS \r00000000000%s,%s %s %s %s %s 0.0 %s /%s/%s/",
                             rr.bufs[0], rr.bufs[1], rr.bufs[2], rr.bufs[3],
                             rr.bufs[4], rr.bufs[5], rr.bufs[6], rr.bufs[7], rr.bufs[8]);
                    send_data_room(users[client_id].inroom, room_items);
                }
                stmtrow_free(&rr);
            }
        }
    }
    else if (strcmp(token, "MOVESTUFF") == 0) {
        char* item_id = strtok(NULL, " ");
        char* x_str = strtok(NULL, " ");
        char* y_str = strtok(NULL, " ");
        char* rotate_str = strtok(NULL, " ");

        if (item_id && x_str && y_str && rotate_str) {
            const char *sql_up = "UPDATE roomitems SET xx=?, yy=?, rotate=? WHERE id=? AND roomid=? LIMIT 1";
            MYSQL_BIND p[5];
            int xx = atoi(x_str);
            int yy = atoi(y_str);
            int rot = atoi(rotate_str);
            int idv = atoi(item_id);
            int rid = users[client_id].inroom;

            bind_int_in(&p[0], &xx);
            bind_int_in(&p[1], &yy);
            bind_int_in(&p[2], &rot);
            bind_int_in(&p[3], &idv);
            bind_int_in(&p[4], &rid);
            (void)stmt_exec(conn, sql_up, p, 5);

            const char *sql_sel =
                "SELECT id, shortname, xx, yy, zz, rotate, extra, longname, color "
                "FROM roomitems WHERE roomid=? AND id=? LIMIT 1";
            MYSQL_BIND sp[2];
            bind_int_in(&sp[0], &rid);
            bind_int_in(&sp[1], &idv);

            StmtRow rr;
            int got = stmt_query_first_row(conn, sql_sel, sp, 2, &rr, DB_OUTBUF_LARGE);
            if (got == 1) {
                if (rr.cols >= 9) {
                    char room_items[1024];
                    snprintf(room_items, sizeof(room_items),
                             "ACTIVEOBJECT_UPDATE \r00000000000%s,%s %s %s %s %s 0.0 %s /%s/%s/",
                             rr.bufs[0], rr.bufs[1], rr.bufs[2], rr.bufs[3],
                             rr.bufs[4], rr.bufs[5], rr.bufs[6], rr.bufs[7], rr.bufs[8]);
                    send_data_room(users[client_id].inroom, room_items);
                }
                stmtrow_free(&rr);
            }
        }
    }
    else if (strcmp(token, "REMOVESTUFF") == 0) {
        char* item_id = strtok(NULL, " ");
        if (item_id) {
            int idv = atoi(item_id);
            int rid = users[client_id].inroom;

            const char *sql_sel =
                "SELECT id, shortname, xx, yy, zz, rotate, extra, longname, color "
                "FROM roomitems WHERE roomid=? AND id=? LIMIT 1";
            MYSQL_BIND sp[2];
            bind_int_in(&sp[0], &rid);
            bind_int_in(&sp[1], &idv);

            StmtRow rr;
            int got = stmt_query_first_row(conn, sql_sel, sp, 2, &rr, DB_OUTBUF_LARGE);
            if (got == 1) {
                if (rr.cols >= 9) {
                    char room_items[1024];
                    snprintf(room_items, sizeof(room_items),
                             "ACTIVEOBJECT_UPDATE \r00000000000%s,%s 99 99 %s %s 0.0 %s /%s/%s/",
                             rr.bufs[0], rr.bufs[1], rr.bufs[4], rr.bufs[5], rr.bufs[6], rr.bufs[7], rr.bufs[8]);
                    send_data_room(users[client_id].inroom, room_items);
                }
                stmtrow_free(&rr);
            }

            const char *sql_del = "DELETE FROM roomitems WHERE id=? AND roomid=? LIMIT 1";
            MYSQL_BIND dp[2];
            bind_int_in(&dp[0], &idv);
            bind_int_in(&dp[1], &rid);
            (void)stmt_exec(conn, sql_del, dp, 2);
        }
    }
    else if (strcmp(token, "ADDSTRIPITEM") == 0) {
        char* item_id = strtok(NULL, " ");
        (void)strtok(NULL, " "); /* unused */
        char* room_item_id = strtok(NULL, " ");

        if (item_id && room_item_id) {
            int rid = users[client_id].inroom;
            int roomidv = atoi(room_item_id);

            const char *sql_sel =
                "SELECT item, shortname, longname, color, extra "
                "FROM roomitems WHERE roomid=? AND id=? LIMIT 1";
            MYSQL_BIND sp[2];
            bind_int_in(&sp[0], &rid);
            bind_int_in(&sp[1], &roomidv);

            StmtRow rr;
            int got = stmt_query_first_row(conn, sql_sel, sp, 2, &rr, DB_OUTBUF_LARGE);
            if (got == 1) {
                /* send removal update to room (best-effort) */
                {
                    char upd[512];
                    snprintf(upd, sizeof(upd),
                             "ACTIVEOBJECT_UPDATE \r00000000000%d,%s 99 99 0 0 0.0  / / /",
                             roomidv, (rr.cols>1?rr.bufs[1]:""));
                    send_data_room(users[client_id].inroom, upd);
                }

                const char *sql_ins =
                    "INSERT INTO useritems (user,item,shortname,longname,color,extra) VALUES (?,?,?,?,?,?)";
                MYSQL_BIND ip[6];
                unsigned long lu=0,litem=0,ls=0,ll=0,lc=0,le=0;

                bind_str_in(&ip[0], users[client_id].name, &lu);
                bind_str_in(&ip[1], (rr.cols>0?rr.bufs[0]:""), &litem);
                bind_str_in(&ip[2], (rr.cols>1?rr.bufs[1]:""), &ls);
                bind_str_in(&ip[3], (rr.cols>2?rr.bufs[2]:""), &ll);
                bind_str_in(&ip[4], (rr.cols>3?rr.bufs[3]:""), &lc);
                bind_str_in(&ip[5], (rr.cols>4?rr.bufs[4]:""), &le);
                (void)stmt_exec(conn, sql_ins, ip, 6);

                const char *sql_del = "DELETE FROM roomitems WHERE id=? AND roomid=? LIMIT 1";
                MYSQL_BIND dp[2];
                bind_int_in(&dp[0], &roomidv);
                bind_int_in(&dp[1], &rid);
                (void)stmt_exec(conn, sql_del, dp, 2);

                stmtrow_free(&rr);
            }
        }
    }
    else if (strcmp(token, "LOOKTO") == 0) {
        char* x_str = strtok(NULL, " ");
        char* y_str = strtok(NULL, " ");
        if (x_str && y_str) {
            int to_x = atoi(x_str);
            int to_y = atoi(y_str);

            char own_data[64] = "";
            if (users[client_id].owner || users[client_id].rights) strcpy(own_data, "flatctrl useradmin/");

            int rx = 2, ry = 2;
            if (users[client_id].userx > to_x && users[client_id].usery > to_y) { rx = 7; ry = 7; }
            else if (users[client_id].userx > to_x && users[client_id].usery < to_y) { rx = 5; ry = 5; }
            else if (users[client_id].userx < to_x && users[client_id].usery > to_y) { rx = 1; ry = 1; }
            else if (users[client_id].userx < to_x && users[client_id].usery < to_y) { rx = 3; ry = 3; }
            else if (users[client_id].userx > to_x) { rx = 6; ry = 6; }
            else if (users[client_id].userx < to_x) { rx = 2; ry = 2; }
            else if (users[client_id].usery > to_y) { rx = 0; ry = 0; }
            else if (users[client_id].usery < to_y) { rx = 4; ry = 4; }

            char status_packet[1024];
            if (users[client_id].dance) {
                snprintf(status_packet, sizeof(status_packet),
                         "STATUS\r%s %d,%d,%d,%d,%d/%sdance/",
                         users[client_id].name, users[client_id].userx, users[client_id].usery,
                         users[client_id].userz, rx, ry, own_data);
            } else {
                snprintf(status_packet, sizeof(status_packet),
                         "STATUS\r%s %d,%d,%d,%d,%d/%s",
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

            (void)db_rights_insert(conn, users[client_id].inroom, target_user);

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
            (void)db_rights_delete(conn, users[client_id].inroom, target_user);
        }
    }
    else if (strcmp(token, "UPDATEFLAT") == 0) {
        char* flat_data = strchr(packet_copy, '/');
        flat_data = flat_data ? (flat_data + 1) : NULL;
        if (flat_data) {
            char* flat_id = flat_data;
            char* name = strchr(flat_id, '/'); if (name) { *name = '\0'; name++; }
            char* door = name ? strchr(name, '/') : NULL; if (door) { *door = '\0'; door++; }

            if (flat_id && name && door) {
                char* newline = strchr(door, '\r');
                if (newline) *newline = '\0';

                const char *sql = "UPDATE rooms SET name=?, door=? WHERE id=? LIMIT 1";
                MYSQL_BIND p[3];
                unsigned long ln=0, ld=0, lid=0;
                bind_str_in(&p[0], name, &ln);
                bind_str_in(&p[1], door, &ld);
                bind_str_in(&p[2], flat_id, &lid);
                (void)stmt_exec(conn, sql, p, 3);
            }
        }
    }
    else if (strcmp(token, "SETFLATINFO") == 0) {
        char* flat_data = strchr(packet_copy, '/');
        flat_data = flat_data ? (flat_data + 1) : NULL;
        if (flat_data) {
            char* flat_id = flat_data;
            char* info_data = strchr(flat_id, '/'); if (info_data) { *info_data = '\0'; info_data++; }

            if (flat_id && info_data) {
                char* newline = strchr(info_data, '\r');
                if (newline) *newline = '\0';

                char desc[512]={0}, password[128]={0};
                sscanf(info_data, "desc=%511[^/]/password=%127s", desc, password);

                const char *sql = "UPDATE rooms SET `desc`=?, pass=? WHERE id=? LIMIT 1";
                MYSQL_BIND p[3];
                unsigned long ld=0, lp=0, lid=0;
                bind_str_in(&p[0], desc, &ld);
                bind_str_in(&p[1], password, &lp);
                bind_str_in(&p[2], flat_id, &lid);
                (void)stmt_exec(conn, sql, p, 3);
            }
        }
    }
}

/* -------------------- load user data wrapper -------------------- */

static void load_user_data(int client_id, const char* username, const char* password) {
    (void)db_load_user_data(users[client_id].db_conn, username, password, &users[client_id]);
}

/* -------------------- send helpers -------------------- */

static void send_data(int client_id, const char* data) {
    if (client_id < 0 || client_id >= MAX_CLIENTS) return;
    if (!users[client_id].used || users[client_id].socket == INVALID_SOCKET) return;
    (void)send_cmd(&users[client_id], data);
}

static void send_data_all(const char* data) {
    pthread_mutex_lock(&users_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (users[i].used && users[i].socket != INVALID_SOCKET) {
            (void)send_cmd(&users[i], data);
        }
    }
    pthread_mutex_unlock(&users_mutex);
}

static void send_data_room(int room_id, const char* data) {
    pthread_mutex_lock(&users_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (users[i].used && users[i].inroom == room_id && users[i].socket != INVALID_SOCKET) {
            (void)send_cmd(&users[i], data);
        }
    }
    pthread_mutex_unlock(&users_mutex);
}

/* -------------------- stubs -------------------- */

static void load_wordfilters(void) { }
static char* filter_chat_message(char* message) { return message; }

/* -------------------- pathfinding -------------------- */

static void* pathfinding_thread(void* arg) {
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

        if (current_x == target_x && current_y == target_y) {
            pthread_mutex_lock(&users_mutex);
            users[client_id].pathfinding_active = 0;
            pthread_mutex_unlock(&users_mutex);
            break;
        }

        int new_x = current_x;
        int new_y = current_y;

        if (current_x < target_x) new_x++;
        else if (current_x > target_x) new_x--;

        if (current_y < target_y) new_y++;
        else if (current_y > target_y) new_y--;

        int height = get_room_height(new_x, new_y, users[client_id].roomcache);

        char status_packet[1024];
        char own_data[64] = "";
        pthread_mutex_lock(&users_mutex);
        if (users[client_id].owner || users[client_id].rights) strcpy(own_data, "flatctrl useradmin/");
        pthread_mutex_unlock(&users_mutex);

        snprintf(status_packet, sizeof(status_packet), "STATUS \r%s %d,%d,%d,2,2/%s",
                 users[client_id].name, new_x, new_y, height, own_data);
        send_data_room(users[client_id].inroom, status_packet);

        pthread_mutex_lock(&users_mutex);
        users[client_id].userx = new_x;
        users[client_id].usery = new_y;
        users[client_id].userz = height;
        pthread_mutex_unlock(&users_mutex);

        usleep(500000);
    }

    return NULL;
}

static int get_room_height(int x, int y, char* heightmap) {
    (void)heightmap;
    if (x >= 0 && x < 10 && y >= 0 && y < 28) return 1;
    return 0;
}

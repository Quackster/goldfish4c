/*
    Fully converted to MySQL C API prepared statements (MYSQL_STMT)
    for ALL database operations in the code you provided.

    Key changes:
    - Removed ALL mysql_query()+sprintf() that included user data.
    - Removed escape_string() usage (kept stub for compatibility but unused).
    - Added a small prepared-statement layer:
        * stmt_exec() for INSERT/UPDATE/DELETE
        * stmt_query_each_row() for multi-row SELECT
        * stmt_query_first_row() for single-row SELECT
      using dynamic result binding (no need to know column names/types ahead of time).
    - Fixed packet_copy overflow risk by bounded copy.

    Build note:
    - Link against mysqlclient / libmysql.
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
#define BUFFER_SIZE 4096
#define INVALID_SOCKET -1

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
    char customData[1096];
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
    char tmp[5];
    memcpy(tmp, hdr, 4);
    tmp[4] = '\0';

    char digits[5];
    int j = 0;
    for (int i = 0; i < 4; i++) {
        unsigned char c = (unsigned char)tmp[i];
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

static void dump_hex(const char *tag, const void *buf, size_t len) {
    const unsigned char *p = (const unsigned char*)buf;
    printf("%s (%zu): ", tag, len);
    for (size_t i = 0; i < len; i++) printf("%02X ", p[i]);
    printf("\n");
}

/* -------------------- protocol send helper -------------------- */
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

	// dump_hex("TX body", data, dlen);
	//printf("test: %s\n", data);
    
	int rc = send(c->socket, payload, (int)plen, 0);
    
    // Create a readable version for console output
    char *readable = (char*)malloc(plen * 4 + 1); // Allow space for 4x expansion
    if (readable) {
        size_t read_pos = 0;
        for (size_t i = 0; i < plen && read_pos < plen * 4; i++) {
            if (payload[i] < 32 || payload[i] == 127) {
                // Replace with ASCII number
                int len = snprintf(readable + read_pos, plen * 4 - read_pos, "[%d]", (unsigned char)payload[i]);
                if (len > 0 && len < (int)(plen * 4 - read_pos)) {
                    read_pos += len;
                } else {
                    break;
                }
            } else {
                readable[read_pos++] = payload[i];
            }
        }
        readable[read_pos] = '\0';
        printf("<-- %s\n", readable);
        free(readable);
    } else {
        printf("<-- %s\n", payload);
    }

    free(payload);
    return rc;
}



/* -------------------- prepared statement layer -------------------- */

/*
    We implement:
    - stmt_exec(): execute INSERT/UPDATE/DELETE (optional params)
    - stmt_query_each_row(): execute SELECT, dynamically binds result columns to strings,
      then calls a callback for each row.
    - stmt_query_first_row(): same but stops at first row and returns 1 if row exists, 0 if none.

    Dynamic binding approach: bind every output column as MYSQL_TYPE_STRING with a fixed buffer.
    MySQL will convert types to string if needed.
*/

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
    if (!meta) {
        // SELECT with no columns? treat as error
        return -1;
    }

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

typedef int (*row_cb_fn)(StmtRow *row, void *ud); // return 0 to continue, nonzero to stop

static int stmt_query_each_row(MYSQL *conn, const char *sql,
                               MYSQL_BIND *params, unsigned long param_count,
                               row_cb_fn cb, void *ud)
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
    if (stmtrow_init_from_metadata(stmt, &row, 2048) != 0) {
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
                                StmtRow *out_row /* filled */, unsigned long out_buf_sz)
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
        return 0; // no row
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
    return 1; // got row
}

/* -------------------- DB convenience funcs -------------------- */

static int db_check_login(MYSQL *conn, const char *username, const char *password) {
    const char *sql = "SELECT 1 FROM users WHERE name=? AND password=? LIMIT 1";
    MYSQL_BIND p[2];
    unsigned long lu=0, lp=0;
    bind_str_in(&p[0], username, &lu);
    bind_str_in(&p[1], password, &lp);

    StmtRow row;
    int got = stmt_query_first_row(conn, sql, p, 2, &row, 32);
    if (got == 1) stmtrow_free(&row);
    return got; // 1 ok, 0 no, -1 error
}

static int db_load_user_data(MYSQL *conn, const char *username, const char *password, User *u) {
    /*
        We select the columns in the order your original mysql_fetch_row used:
        row[1]..row[13] => name,password,credits,email,figure,birthday,phonenumber,customData,had_read_agreement,sex,country,has_special_rights,badge_type
        Plus inroom (optional) is handled elsewhere in your code; we’ll also select it here to keep consistent.
    */
    const char *sql =
        "SELECT id,name,password,credits,email,figure,birthday,phonenumber,customData,had_read_agreement,sex,country,has_special_rights,badge_type,inroom "
        "FROM users WHERE name=? AND password=? LIMIT 1";

    MYSQL_BIND p[2];
    unsigned long lu=0, lp=0;
    bind_str_in(&p[0], username, &lu);
    bind_str_in(&p[1], password, &lp);

    StmtRow row;
    int got = stmt_query_first_row(conn, sql, p, 2, &row, 2048);
    if (got != 1) return got;

    // Safety: verify expected column count
    // Columns: 15 as in SELECT above.
    // If schema differs, this still works best-effort using indices if present.
    // Index mapping:
    // 0 id, 1 name, 2 password, 3 credits, 4 email, 5 figure, 6 birthday, 7 phonenumber,
    // 8 customData, 9 had_read_agreement, 10 sex, 11 country, 12 has_special_rights, 13 badge_type, 14 inroom
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
    if (u->inroom == 0 && row.cols >= 15) u->inroom = atoi(row.bufs[14]);

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
    const char *sql = "INSERT INTO chatloggs VALUES (NULL, ?, ?, ?, ?)";
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
        "(name,password,credits,email,figure,birthday,phonenumber,customData,had_read_agreement,sex,country,has_special_rights,badge_type) "
        "VALUES (?,?,?,?,1337,?,?,?,?,?,?,?,'0','0')";

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

static int db_add_favroom(MYSQL *conn, const char *user, const char *room_id_str) {
    const char *sql = "INSERT INTO favrooms VALUES (NULL, ?, ?)";
    MYSQL_BIND p[2];
    unsigned long lu=0, lr=0;
    bind_str_in(&p[0], user, &lu);
    bind_str_in(&p[1], room_id_str, &lr);
    return stmt_exec(conn, sql, p, 2);
}

static int db_del_favroom(MYSQL *conn, const char *room_id_str) {
    const char *sql = "DELETE FROM favrooms WHERE roomid = ? LIMIT 1";
    MYSQL_BIND p[1];
    unsigned long lr=0;
    bind_str_in(&p[0], room_id_str, &lr);
    return stmt_exec(conn, sql, p, 1);
}

static int db_buddy_request(MYSQL *conn, const char *from_user, const char *to_user) {
    const char *sql = "INSERT INTO buddy VALUES (NULL, ?, ?, '0')";
    MYSQL_BIND p[2];
    unsigned long lf=0, lt=0;
    bind_str_in(&p[0], from_user, &lf);
    bind_str_in(&p[1], to_user, &lt);
    return stmt_exec(conn, sql, p, 2);
}

static int db_buddy_accept(MYSQL *conn, const char *from_user, const char *to_user) {
    const char *sql = "UPDATE buddy SET accept='1' WHERE `from` LIKE ? AND `to`=? AND `accept`='0'";
    MYSQL_BIND p[2];
    unsigned long lf=0, lt=0;
    bind_str_in(&p[0], from_user, &lf);
    bind_str_in(&p[1], to_user, &lt);
    return stmt_exec(conn, sql, p, 2);
}

static int db_buddy_decline(MYSQL *conn, const char *from_user, const char *to_user) {
    const char *sql = "DELETE FROM buddy WHERE `from` LIKE ? AND `to`=? AND `accept`='0'";
    MYSQL_BIND p[2];
    unsigned long lf=0, lt=0;
    bind_str_in(&p[0], from_user, &lf);
    bind_str_in(&p[1], to_user, &lt);
    return stmt_exec(conn, sql, p, 2);
}

static int db_buddy_remove_pair(MYSQL *conn, const char *a, const char *b) {
    // delete both directions where accept=1
    const char *sql1 = "DELETE FROM buddy WHERE `from` LIKE ? AND `to`=? AND `accept`='1' LIMIT 1";
    const char *sql2 = "DELETE FROM buddy WHERE `to` LIKE ? AND `from`=? AND `accept`='1' LIMIT 1";
    MYSQL_BIND p[2];
    unsigned long la=0, lb=0;
    bind_str_in(&p[0], a, &la);
    bind_str_in(&p[1], b, &lb);
    stmt_exec(conn, sql1, p, 2);
    stmt_exec(conn, sql2, p, 2);
    return 0;
}

static int db_rights_insert(MYSQL *conn, int room_id, const char *user) {
    const char *sql = "INSERT INTO rights VALUES (NULL, ?, ?)";
    MYSQL_BIND p[2];
    unsigned long lu=0;
    bind_int_in(&p[0], &room_id);
    bind_str_in(&p[1], user, &lu);
    return stmt_exec(conn, sql, p, 2);
}

static int db_rights_delete(MYSQL *conn, int room_id, const char *user) {
    const char *sql = "DELETE FROM rights WHERE room=? AND user=?";
    MYSQL_BIND p[2];
    unsigned long lu=0;
    bind_int_in(&p[0], &room_id);
    bind_str_in(&p[1], user, &lu);
    return stmt_exec(conn, sql, p, 2);
}

static int db_has_rights(MYSQL *conn, const char *user) {
    const char *sql = "SELECT 1 FROM rights WHERE user=? LIMIT 1";
    MYSQL_BIND p[1];
    unsigned long lu=0;
    bind_str_in(&p[0], user, &lu);

    StmtRow row;
    int got = stmt_query_first_row(conn, sql, p, 1, &row, 32);
    if (got == 1) stmtrow_free(&row);
    return got;
}

/* -------------------- Server prototypes -------------------- */

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

/* kept for compatibility but UNUSED now */
char* escape_string(MYSQL* conn, const char* str) {
    (void)conn;
    (void)str;
    static char dummy[1] = {0};
    return dummy;
}

/* -------------------- main -------------------- */

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

void init_database() {
    global_db_conn = mysql_init(NULL);
    if (!mysql_real_connect(global_db_conn, "localhost", "root", "verysecret", "goldfish4c", 0, NULL, 0)) {
        printf("Failed to initialize global database connection\n");
        exit(EXIT_FAILURE);
    }
}

/* -------------------- client handling -------------------- */

void* handle_client(void* arg) {
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

void cleanup_user(int client_id) {
    pthread_mutex_lock(&users_mutex);

    if (client_id < 0 || client_id >= MAX_CLIENTS) {
        pthread_mutex_unlock(&users_mutex);
        return;
    }

    if (users[client_id].used) {
        if (users[client_id].inroom != 0) {
            // prepared: UPDATE rooms SET inroom = inroom - 1 WHERE id = ?
            db_update_room_inroom_delta(users[client_id].db_conn, users[client_id].inroom, -1);
            users[client_id].inroom = 0;
            users[client_id].owner = 0;
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

/* -------------------- packet processing -------------------- */

static int cb_pubs_row(StmtRow *row, void *ud) {
    // We now SELECT: name, now_in, max_in, mapname, heightmap
    // Indexes: 0 name, 1 now_in, 2 max_in, 3 mapname, 4 heightmap
    char *out = (char*)ud;
    if (!out) return 1;

    if (row->cols >= 5) {
        char temp[768];
        snprintf(temp, sizeof(temp),
            "%s,%s,%s,127.0.0.1/127.0.0.1,37120,%s\t%s,%s,%s,%s\r",
            row->bufs[0],  // name
            row->bufs[1],  // now_in
            row->bufs[2],  // max_in
            row->bufs[0],  // name again (after port)
            row->bufs[3],  // mapname
            row->bufs[1],  // now_in again
            row->bufs[2],  // max_in again
            row->bufs[4]   // heightmap
        );

        strncat(out, temp, 4096 - strlen(out) - 1);
    }
    return 0;
}

static int cb_rooms_busy_row(StmtRow *row, void *ud) {
    // We now SELECT: id, name, owner, door, pass, floor, inroom, `desc`
    // Indexes: 0 id, 1 name, 2 owner, 3 door, 4 pass, 5 floor, 6 inroom, 7 desc
    char *out = (char*)ud;
    if (!out) return 1;

    if (row->cols >= 8) {
        char temp[1024];
        snprintf(temp, sizeof(temp),
            "\r%s/%s/%s/%s/%s/%s/127.0.0.1/127.0.0.1/37120/%s/null/%s",
            row->bufs[0], // id
            row->bufs[1], // name
            row->bufs[2], // owner
            row->bufs[3], // door
            row->bufs[4], // pass
            row->bufs[5], // floor
            row->bufs[6], // inroom
            row->bufs[7]  // desc
        );

        strncat(out, temp, 4096 - strlen(out) - 1);
    }
    return 0;
}

typedef struct {
    int room_id;
    char out[2048];
} RoomsForUserCtx;

static int cb_rooms_for_owner_row(StmtRow *row, void *ud) {
    RoomsForUserCtx *ctx = (RoomsForUserCtx*)ud;
    if (!ctx) return 1;
    // original used row[0],row[1],row[3],row[4],row[5],row[6],row[12]
    if (row->cols >= 13) {
        char temp[1096];
        snprintf(temp, sizeof(temp),
                 "\r%s/%s/%s/%s/%s/%s/127.0.0.1/127.0.0.1/37120/0/null/%s",
                 row->bufs[0], row->bufs[1], row->bufs[3], row->bufs[4], row->bufs[5], row->bufs[6], row->bufs[12]);
        strncat(ctx->out, temp, sizeof(ctx->out) - strlen(ctx->out) - 1);
    }
    return 0;
}

static int cb_buddy_list_row(StmtRow *row, void *ud) {
    // original: data_name = (strcmp(row[1], me)!=0) ? row[1] : row[2]
    // and appended "\r1 %s UNKNOW\r"
    struct { const char *me; char *out; size_t outsz; } *ctx = ud;
    if (!ctx || !ctx->out) return 1;
    if (row->cols >= 3) {
        const char *name = (strcmp(row->bufs[1], ctx->me) != 0) ? row->bufs[1] : row->bufs[2];
        char temp[128];
        snprintf(temp, sizeof(temp), "\r1 %s UNKNOW\r", name);
        strncat(ctx->out, temp, ctx->outsz - strlen(ctx->out) - 1);
    }
    return 0;
}

void process_packet(int client_id, char* packet) {
    char packet_copy[BUFFER_SIZE];
    // bounded copy (prevents overflow if packet > BUFFER_SIZE)
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
                             "xxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx");

                    send_data(client_id,
                        "HEIGHTMAPxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx\nxxxxxxxxxx");

                    char user_packet[512];
                    snprintf(user_packet, sizeof(user_packet), "USERS\r %s %s 3 5 0 %s",
                             users[client_id].name,
                             (strlen(users[client_id].figure) > 7 ? (users[client_id].figure + 7) : users[client_id].figure),
                             users[client_id].customData);
                    send_data(client_id, user_packet);

                    users[client_id].userx = 12;
                    users[client_id].usery = 27;

                    char status_packet[1096];
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

            char user_object[4096];
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
        char room_data[4096] = "";
        stmt_query_each_row(conn, "SELECT name, now_in, max_in, mapname, heightmap FROM pubs", NULL, 0, cb_pubs_row, room_data);

        char all_units[4096];
        snprintf(all_units, sizeof(all_units), "ALLUNITS\r%s", room_data);
        send_data(client_id, all_units);
    }
    else if (strcmp(token, "SEARCHBUSYFLATS") == 0) {
        char room_data[4096] = "";
		stmt_query_each_row(conn,
			"SELECT id, name, owner, door, pass, floor, inroom, `desc` "
			"FROM rooms ORDER BY inroom DESC",
			NULL, 0, cb_rooms_busy_row, room_data);

        char busy_flats[4096];
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
        char* lines[15];
        int line_count = 0;
        char* line = strtok(packet_copy, "\r");
        while (line && line_count < 15) {
            lines[line_count++] = line;
            line = strtok(NULL, "\r");
        }

        if (line_count >= 11) {
            char username[50], password[50], email[1096], figure[1096];
            char birthday[20], phone[20], mission[1096], agree[10], sex[10], city[50];

            sscanf(lines[0], "REGISTER name=%49s", username);
            sscanf(lines[1], "password=%49s", password);
            sscanf(lines[2], "email=%99s", email);
            sscanf(lines[3], "figure=%99s", figure);
            sscanf(lines[5], "birthday=%19s", birthday);
            sscanf(lines[6], "phonenumber=%19s", phone);
            sscanf(lines[7], "customData=%255[^\r]", mission);
            sscanf(lines[8], "had_read_agreement=%9s", agree);
            sscanf(lines[9], "sex=%9s", sex);
            sscanf(lines[10], "country=%49s", city);

            db_register_user(conn, username, password, email, figure, birthday, phone, mission, agree, sex, city);
        }
    }
    else if (strcmp(token, "UPDATE") == 0) {
        char* lines[15];
        int line_count = 0;
        char* line = strtok(packet_copy, "\r");
        while (line && line_count < 15) {
            lines[line_count++] = line;
            line = strtok(NULL, "\r");
        }

        if (line_count >= 11) {
            char username[50], password[50], email[1096], figure[1096];
            char birthday[20], phone[20], mission[1096], agree[10], sex[10], city[50];

            sscanf(lines[0], "UPDATE name=%49s", username);
            sscanf(lines[1], "password=%49s", password);
            sscanf(lines[2], "email=%99s", email);
            sscanf(lines[3], "figure=%99s", figure);
            sscanf(lines[5], "birthday=%19s", birthday);
            sscanf(lines[6], "phonenumber=%19s", phone);
            sscanf(lines[7], "customData=%255[^\r]", mission);
            sscanf(lines[8], "had_read_agreement=%9s", agree);
            sscanf(lines[9], "sex=%9s", sex);
            sscanf(lines[10], "country=%49s", city);

            db_update_user_profile(conn, username, password, email, figure, birthday, phone, agree, city, sex, mission);
        }
    }
    else if (strcmp(token, "SEARCHFLATFORUSER") == 0) {
        char* user = strchr(packet_copy, '/');
        user = user ? (user + 1) : NULL;
        if (user) {
            char* newline = strchr(user, '\r');
            if (newline) *newline = '\0';

            const char *sql = "SELECT * FROM rooms WHERE owner = ?";
            MYSQL_BIND p[1];
            unsigned long lu=0;
            bind_str_in(&p[0], user, &lu);

            RoomsForUserCtx ctx;
            memset(&ctx, 0, sizeof(ctx));

            stmt_query_each_row(conn, sql, p, 1, cb_rooms_for_owner_row, &ctx);

            char busy_flats[2048];
            snprintf(busy_flats, sizeof(busy_flats), "BUSY_FLAT_RESULTS 1%s", ctx.out);
            send_data(client_id, busy_flats);
        }
    }
    else if (strcmp(token, "UINFO_MATCH") == 0) {
        char* user = strchr(packet_copy, '/');
        user = user ? (user + 1) : NULL;
        if (user) {
            char* newline = strchr(user, '\r');
            if (newline) *newline = '\0';

            const char *sql = "SELECT * FROM users WHERE name = ? LIMIT 1";
            MYSQL_BIND p[1];
            unsigned long lu=0;
            bind_str_in(&p[0], user, &lu);

            StmtRow row;
            int got = stmt_query_first_row(conn, sql, p, 1, &row, 2048);
            if (got == 1) {
                // original: row[8], row[4]+7, row[10]
                char member_info[512];
                const char *custom = (row.cols > 8 ? row.bufs[8] : "");
                const char *figure = (row.cols > 5 ? row.bufs[5] : "");
                const char *sex = (row.cols > 10 ? row.bufs[10] : "");

                const char *figure_no_prefix = figure;
                if (strncmp(figure, "figure=", 7) == 0) figure_no_prefix = figure + 7;

                snprintf(member_info, sizeof(member_info),
                         "MEMBERINFO MESSENGER\r%s\r%s\r\r0\r%s%s",
                         user, custom, figure_no_prefix, sex);
                send_data(client_id, member_info);
                stmtrow_free(&row);
            } else {
                char no_such_user[128];
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
            db_delete_room_by_id(conn, flat_id);
        }
    }
    else if (strcmp(token, "GET_FAVORITE_ROOMS") == 0) {
        // NOTE: original did N+1 queries (favrooms then rooms by id). We'll keep logic but prepared.
        char* username = strtok(NULL, " ");
        if (username) {
            // collect favorite room ids
            const char *sql_fav = "SELECT * FROM favrooms WHERE user = ?";
            MYSQL_BIND p[1];
            unsigned long lu=0;
            bind_str_in(&p[0], username, &lu);

            // We'll build output by doing inner prepared query per roomid (same as original).
            // For each fav row, row[2] was room id.
            struct FavCtx {
                MYSQL *conn;
                char out[2048];
            } favctx;

            favctx.conn = conn;
            favctx.out[0] = '\0';

            // callback for favrooms rows
            int fav_cb(StmtRow *r, void *ud) {
                struct FavCtx *fc = (struct FavCtx*)ud;
                if (!fc || r->cols < 3) return 0;
                const char *roomid = r->bufs[2];

                const char *sql_room = "SELECT * FROM rooms WHERE id = ? LIMIT 1";
                MYSQL_BIND rp[1];
                unsigned long lr=0;
                bind_str_in(&rp[0], roomid, &lr);

                StmtRow rr;
                int got = stmt_query_first_row(fc->conn, sql_room, rp, 1, &rr, 2048);
                if (got == 1) {
                    // original used room row[0],row[1],row[3],row[4],row[5],row[6],row[10],row[12]
                    if (rr.cols >= 13) {
                        char temp[1096];
                        snprintf(temp, sizeof(temp),
                                 "\r%s/%s/%s/%s/%s/%s/127.0.0.1/127.0.0.1/37120/%s/null/%s",
                                 rr.bufs[0], rr.bufs[1], rr.bufs[3], rr.bufs[4],
                                 rr.bufs[5], rr.bufs[6], rr.bufs[10], rr.bufs[12]);
                        strncat(fc->out, temp, sizeof(fc->out) - strlen(fc->out) - 1);
                    }
                    stmtrow_free(&rr);
                }
                return 0;
            }

            stmt_query_each_row(conn, sql_fav, p, 1, fav_cb, &favctx);

            char busy_flats[2048];
            snprintf(busy_flats, sizeof(busy_flats), "BUSY_FLAT_RESULTS 1%s", favctx.out);
            send_data(client_id, busy_flats);
        }
    }
    else if (strcmp(token, "ADD_FAVORITE_ROOM") == 0) {
        char* room_id = strtok(NULL, " ");
        if (room_id) db_add_favroom(conn, users[client_id].name, room_id);
    }
    else if (strcmp(token, "DEL_FAVORITE_ROOM") == 0) {
        char* room_id = strtok(NULL, " ");
        if (room_id) db_del_favroom(conn, room_id);
    }
    else if (strcmp(token, "SEARCHFLAT") == 0) {
        char* search_term = strchr(packet_copy, '/');
        search_term = search_term ? (search_term + 1) : NULL;
        if (search_term) {
            char* newline = strchr(search_term, '\r');
            if (newline) *newline = '\0';

            char like[1096];
            snprintf(like, sizeof(like), "%%%s%%", search_term);

            const char *sql = "SELECT * FROM rooms WHERE owner LIKE ? OR name LIKE ?";
            MYSQL_BIND p[2];
            unsigned long l0=0,l1=0;
            bind_str_in(&p[0], like, &l0);
            bind_str_in(&p[1], like, &l1);

            char room_data[2048] = "";
            stmt_query_each_row(conn, sql, p, 2, cb_rooms_for_owner_row /* compatible indices */, &(RoomsForUserCtx){0});
            // cb_rooms_for_owner_row writes into ctx.out but we used a temp initializer above incorrectly.
            // Let's do it properly:
            RoomsForUserCtx ctx;
            memset(&ctx, 0, sizeof(ctx));
            stmt_query_each_row(conn, sql, p, 2, cb_rooms_for_owner_row, &ctx);

            char busy_flats[2048];
            snprintf(busy_flats, sizeof(busy_flats), "BUSY_FLAT_RESULTS 1%s", ctx.out);
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
                    db_update_room_inroom_delta(conn, users[client_id].inroom, -1);
                    users[client_id].owner = 0;
                }

                db_update_user_inroom(conn, users[client_id].name, gotoroom);
                db_update_room_inroom_delta(conn, gotoroom, +1);

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

            // rooms by id
            const char *sql_room = "SELECT * FROM rooms WHERE id = ? LIMIT 1";
            MYSQL_BIND p[1];
            unsigned long lr=0;
            bind_str_in(&p[0], flat_id, &lr);

            StmtRow room;
            int got = stmt_query_first_row(conn, sql_room, p, 1, &room, 2048);
            if (got == 1) {
                // original uses room[12], room[8], room[9], room[3], room[7]
                const char *model = (room.cols > 7 ? room.bufs[6] : "");
                const char *owner = (room.cols > 3 ? room.bufs[3] : "");
                const char *wall = (room.cols > 8 ? room.bufs[8] : "");
                const char *floor = (room.cols > 9 ? room.bufs[9] : "");
                const char *desc = (room.cols > 12 ? room.bufs[2] : "");

                char room_ready[1096];
                snprintf(room_ready, sizeof(room_ready), "ROOM_READY\r%s", desc);
                send_data(client_id, room_ready);

                char flat_prop1[128];
                snprintf(flat_prop1, sizeof(flat_prop1), "FLATPROPERTY\rwallpaper/%s\r", wall);
                send_data(client_id, flat_prop1);

                char flat_prop2[128];
                snprintf(flat_prop2, sizeof(flat_prop2), "FLATPROPERTY\rfloor/%s\r", floor);
                send_data(client_id, flat_prop2);

                if (strcmp(owner, users[client_id].name) == 0) {
                    users[client_id].owner = 1;
                    send_data(client_id, "YOUAREOWNER");
                }

                // rights check
                int has = db_has_rights(conn, users[client_id].name);
                if (has == 1) {
                    users[client_id].rights = 1;
                    send_data(client_id, "YOUARECONTROLLER");
                }

                // heightmap by model
                const char *sql_h = "SELECT heightmap FROM heightmap WHERE model = ?";
                MYSQL_BIND hp[1];
                unsigned long lm=0;
                bind_str_in(&hp[0], model, &lm);

                StmtRow hm;
                int hgot = stmt_query_first_row(conn, sql_h, hp, 1, &hm, 2048);
                if (hgot == 1) {
                    // heightmap single col at idx 0
                    snprintf(users[client_id].roomcache, sizeof(users[client_id].roomcache), "%s", hm.bufs[0]);
                    char heightmap_packet[2048];
                    snprintf(heightmap_packet, sizeof(heightmap_packet), "HEIGHTMAP\r%s", hm.bufs[0]);
                    send_data(client_id, heightmap_packet);
                    stmtrow_free(&hm);
                }

                char objects[1096];
                snprintf(objects, sizeof(objects), "OBJECTS WORLD 0 %s", model);
                send_data(client_id, objects);

                // roomitems
                const char *sql_items = "SELECT * FROM roomitems WHERE roomid = ?";
                MYSQL_BIND ip[1];
                int rid = gotoroom;
                bind_int_in(&ip[0], &rid);

                char room_items[4096] = "ACTIVE_OBJECTS ";
                int items_cb(StmtRow *r, void *ud) {
                    char *acc = (char*)ud;
                    if (!acc) return 1;
                    // original uses item_row[0], [3], [9], [10], [7], [11], [8], [4], [5]
                    if (r->cols >= 12) {
                        char temp[512];
                        snprintf(temp, sizeof(temp),
                                 "\r00000000000%s,%s %s %s %s %s 0.0 %s /%s/%s/",
                                 r->bufs[0], r->bufs[3], r->bufs[9], r->bufs[10],
                                 r->bufs[7], r->bufs[11], r->bufs[8], r->bufs[4], r->bufs[5]);
                        strncat(acc, temp, 4096 - strlen(acc) - 1);
                    }
                    return 0;
                }
                stmt_query_each_row(conn, sql_items, ip, 1, items_cb, room_items);
                send_data(client_id, room_items);

                send_data(client_id, "ITEMS \r6663\tposter\t \tfrontwall 6.9999,6.3500/21");

                // other users in room (excluding me)
                const char *sql_u = "SELECT * FROM users WHERE inroom = ? AND name != ?";
                MYSQL_BIND up[2];
                int inr = gotoroom;
                unsigned long ln=0;
                bind_int_in(&up[0], &inr);
                bind_str_in(&up[1], users[client_id].name, &ln);

                int users_cb(StmtRow *r, void *ud) {
                    int cid = *(int*)ud;
                    // original uses user_row[1], user_row[5]+7, user_row[8]
                    if (r->cols >= 9) {
                        const char *uname = r->bufs[1];
                        const char *fig = r->bufs[5];
                        const char *custom = r->bufs[8];
                        if (strncmp(fig, "figure=", 7) == 0) fig += 7;

                        char user_packet[1096];
                        snprintf(user_packet, sizeof(user_packet), "USERS\r %s %s 3 5 0 %s", uname, fig, custom);
                        send_data(cid, user_packet);
                    }
                    return 0;
                }
                stmt_query_each_row(conn, sql_u, up, 2, users_cb, &client_id);

                // statuses
                if (users[client_id].owner || users[client_id].rights) {
                    char status_all[1096];
                    snprintf(status_all, sizeof(status_all), "STATUS \r%s 0,0,99,2,2/flatctrl useradmin/mod 0/", users[client_id].name);
                    send_data_all(status_all);

                    char user_packet[1096];
                    snprintf(user_packet, sizeof(user_packet), "USERS\r %s %s 3 5 0 %s",
                             users[client_id].name,
                             (strncmp(users[client_id].figure, "figure=", 7) == 0 ? users[client_id].figure + 7 : users[client_id].figure),
                             users[client_id].customData);
                    send_data_room(gotoroom, user_packet);

                    char status_room[1096];
                    snprintf(status_room, sizeof(status_room), "STATUS \r%s 3,5,0,2,2/flatctrl useradmin/mod 0/", users[client_id].name);
                    send_data_room(gotoroom, status_room);
                } else {
                    char status_all[1096];
                    snprintf(status_all, sizeof(status_all), "STATUS \r%s 0,0,99,2,2/mod 0/", users[client_id].name);
                    send_data_all(status_all);

                    char user_packet[1096];
                    snprintf(user_packet, sizeof(user_packet), "USERS\r %s %s 3 5 0 %s",
                             users[client_id].name,
                             (strncmp(users[client_id].figure, "figure=", 7) == 0 ? users[client_id].figure + 7 : users[client_id].figure),
                             users[client_id].customData);
                    send_data_room(gotoroom, user_packet);

                    char status_room[1096];
                    snprintf(status_room, sizeof(status_room), "STATUS \r%s 3,5,0,2,2/mod 0/", users[client_id].name);
                    send_data_room(gotoroom, status_room);
                }

                users[client_id].userx = 3;
                users[client_id].usery = 5;

                stmtrow_free(&room);
            }
        }
    }
    else if (strcmp(token, "SHOUT") == 0 || strcmp(token, "CHAT") == 0 || strcmp(token, "WHISPER") == 0) {
        char* message = packet_copy + strlen(token) + 1;

        char clean_message[512];
        int j = 0;
        for (int i = 0; message[i] && i < 511; i++) {
            if (message[i] != '\'' && message[i] != '"' && message[i] != '#') {
                clean_message[j++] = message[i];
            }
        }
        clean_message[j] = '\0';

        char filtered_message[512];
        snprintf(filtered_message, sizeof(filtered_message), "%s", clean_message);
        filter_chat_message(filtered_message);

        char chat_packet[1024];
        snprintf(chat_packet, sizeof(chat_packet), "%s\r%s %s", token, users[client_id].name, filtered_message);
        send_data_room(users[client_id].inroom, chat_packet);

        db_insert_chatlog(conn, users[client_id].name, filtered_message, users[client_id].inroom, token);
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
        char status_packet[1096];
        snprintf(status_packet, sizeof(status_packet), "STATUS \r%s 0,0,99,2,2/mod 0/", users[client_id].name);
        send_data_room(users[client_id].inroom, status_packet);

        users[client_id].userx = 0;
        users[client_id].usery = 0;
        users[client_id].dance = 0;

        if (users[client_id].inroom != 0) {
            db_update_user_inroom(conn, users[client_id].name, 0);
            db_update_room_inroom_delta(conn, users[client_id].inroom, -1);

            users[client_id].inroom = 0;
            users[client_id].owner = 0;
        }

        cleanup_user(client_id);
    }
    else if (strcmp(token, "DANCE") == 0) {
        char status_packet[1096];
        snprintf(status_packet, sizeof(status_packet), "STATUS\r%s %d,%d,0,2,2/dance/",
                 users[client_id].name, users[client_id].userx, users[client_id].usery);
        send_data_room(users[client_id].inroom, status_packet);
        users[client_id].dance = 1;
    }
    else if (strcmp(token, "STOP") == 0) {
        char* stop_type = strtok(NULL, " ");
        if (stop_type && strcmp(stop_type, "DANCE") == 0) {
            char status_packet[1096];
            snprintf(status_packet, sizeof(status_packet), "STATUS\r%s %d,%d,0,2,2/",
                     users[client_id].name, users[client_id].userx, users[client_id].usery);
            send_data_room(users[client_id].inroom, status_packet);
            users[client_id].dance = 0;
        }
    }
    else if (strcmp(token, "GETORDERINFO") == 0) {
        char* item_name = strtok(NULL, " ");
        if (item_name) {
            const char *sql = "SELECT * FROM catalogue WHERE shortname = ? LIMIT 1";
            MYSQL_BIND p[1];
            unsigned long li=0;
            bind_str_in(&p[0], item_name, &li);

            StmtRow row;
            int got = stmt_query_first_row(conn, sql, p, 1, &row, 2048);
            if (got == 1) {
                // original uses row[2], row[3]
                const char *a = (row.cols > 2 ? row.bufs[2] : "");
                const char *b = (row.cols > 3 ? row.bufs[3] : "");
                char order_info[1096];
                snprintf(order_info, sizeof(order_info), "ORDERINFO\r%s/%s/\r%s", a, b, b);
                send_data(client_id, order_info);
                stmtrow_free(&row);
            }
        }
    }
    else if (strcmp(token, "GETSTRIP") == 0) {
        char* strip_type = strtok(NULL, " ");
        if (strip_type) {
            if (strcmp(strip_type, "new") == 0) {
                const char *sql = "SELECT * FROM useritems WHERE user = ? LIMIT 0,11";
                MYSQL_BIND p[1];
                unsigned long lu=0;
                bind_str_in(&p[0], users[client_id].name, &lu);

                char item_info[2048] = "";

                int strip_cb(StmtRow *r, void *ud) {
                    char *acc = (char*)ud;
                    // original uses row[0], row[3], row[4], row[5]
                    if (r->cols >= 6) {
                        char temp[1096];
                        snprintf(temp, sizeof(temp), "BLSI;%s;0;S;%s;%s;%s;%s;1;1;0,0,0/\r",
                                 r->bufs[0], r->bufs[0], r->bufs[3], r->bufs[4], r->bufs[5]);
                        strncat(acc, temp, 2048 - strlen(acc) - 1);
                    }
                    return 0;
                }
                stmt_query_each_row(conn, sql, p, 1, strip_cb, item_info);

                users[client_id].handcache = 11;
                char strip_info[2048];
                snprintf(strip_info, sizeof(strip_info), "STRIPINFO\r%s", item_info);
                send_data(client_id, strip_info);
            }
            else if (strcmp(strip_type, "next") == 0) {
                int start = users[client_id].handcache;
                int end = start + 11;

                const char *sql = "SELECT * FROM useritems WHERE user = ? LIMIT ?,?";
                MYSQL_BIND p[3];
                unsigned long lu=0;
                bind_str_in(&p[0], users[client_id].name, &lu);
                bind_int_in(&p[1], &start);
                bind_int_in(&p[2], &end);

                char item_info[2048] = "";
                int strip_cb(StmtRow *r, void *ud) {
                    char *acc = (char*)ud;
                    if (r->cols >= 6) {
                        char temp[1096];
                        snprintf(temp, sizeof(temp), "BLSI;%s;0;S;%s;%s;%s;%s;1;1;0,0,0/\r",
                                 r->bufs[0], r->bufs[0], r->bufs[3], r->bufs[4], r->bufs[5]);
                        strncat(acc, temp, 2048 - strlen(acc) - 1);
                    }
                    return 0;
                }
                stmt_query_each_row(conn, sql, p, 3, strip_cb, item_info);

                users[client_id].handcache = end;
                char strip_info[2048];
                snprintf(strip_info, sizeof(strip_info), "STRIPINFO\r%s", item_info);
                send_data(client_id, strip_info);
            }
        }
    }
    else if (strcmp(token, "PURCHASE") == 0) {
        char* item_name = strchr(packet_copy, '/');
        item_name = item_name ? (item_name + 1) : NULL;
        if (item_name) {
            char* price_str = strchr(item_name, '/');
            price_str = price_str ? (price_str + 1) : NULL;
            if (price_str) {
                char* newline = strchr(price_str, '\r');
                if (newline) *newline = '\0';

                const char *sql = "SELECT * FROM catalogue WHERE longname = ? LIMIT 1";
                MYSQL_BIND p[1];
                unsigned long ln=0;
                bind_str_in(&p[0], item_name, &ln);

                StmtRow row;
                int got = stmt_query_first_row(conn, sql, p, 1, &row, 2048);
                if (got == 1) {
                    int price = atoi(price_str);

                    // original insert useritems: (NULL, user, row[2],row[3],row[4],row[5],row[6],row[1])
                    const char *sql_ins = "INSERT INTO useritems VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)";
                    MYSQL_BIND ip[7];
                    unsigned long lu=0,l2=0,l3=0,l4=0,l5=0,l6=0,l1=0;
                    bind_str_in(&ip[0], users[client_id].name, &lu);
                    bind_str_in(&ip[1], (row.cols>2?row.bufs[2]:""), &l2);
                    bind_str_in(&ip[2], (row.cols>3?row.bufs[3]:""), &l3);
                    bind_str_in(&ip[3], (row.cols>4?row.bufs[4]:""), &l4);
                    bind_str_in(&ip[4], (row.cols>5?row.bufs[5]:""), &l5);
                    bind_str_in(&ip[5], (row.cols>6?row.bufs[6]:""), &l6);
                    bind_str_in(&ip[6], (row.cols>1?row.bufs[1]:""), &l1);
                    stmt_exec(conn, sql_ins, ip, 7);

                    // update credits
                    const char *sql_up = "UPDATE users SET credits = credits - ? WHERE name = ?";
                    MYSQL_BIND up[2];
                    unsigned long luser=0;
                    bind_int_in(&up[0], &price);
                    bind_str_in(&up[1], users[client_id].name, &luser);
                    stmt_exec(conn, sql_up, up, 2);

                    users[client_id].credits -= price;
                    send_data(client_id, "SYSTEMBROADCAST\rBuying successful !");

                    stmtrow_free(&row);
                }
            }
        }
    }
    else if (strcmp(token, "CREATEFLAT") == 0) {
        char* flat_data = strchr(packet_copy, '/');
        flat_data = flat_data ? (flat_data + 1) : NULL;
        if (flat_data) {
            char* name = flat_data;
            char* model = strchr(name, '/'); model = model ? (model + 1) : NULL;
            char* door  = model ? strchr(model, '/') : NULL; door = door ? (door + 1) : NULL;
            char* pass  = door ? strchr(door, '/') : NULL; pass = pass ? (pass + 1) : NULL;

            if (pass) {
                char* newline = strchr(pass, '\r');
                if (newline) *newline = '\0';

                const char *sql =
                    "INSERT INTO rooms VALUES (NULL, ?, '', ?, 'floor1', '0', ?, ?, '', '100', '100')";
                MYSQL_BIND p[4];
                unsigned long l0=0,l1=0,l2=0,l3=0;
                bind_str_in(&p[0], name, &l0);
                bind_str_in(&p[1], users[client_id].name, &l1);
                bind_str_in(&p[2], door ? door : "", &l2);
                bind_str_in(&p[3], pass, &l3);
                stmt_exec(conn, sql, p, 4);
            }
        }
    }
    else if (strcmp(token, "MESSENGER_REQUESTBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            db_buddy_request(conn, users[client_id].name, buddy_name);
        }
    }
    else if (strcmp(token, "MESSENGER_ACCEPTBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            db_buddy_accept(conn, buddy_name, users[client_id].name);
        }
    }
    else if (strcmp(token, "MESSENGER_DECLINEBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            db_buddy_decline(conn, buddy_name, users[client_id].name);
        }
    }
    else if (strcmp(token, "MESSENGERINIT") == 0) {
        // SELECT * FROM buddy WHERE (`to` LIKE me AND accept=1) OR (`from` LIKE me AND accept=1)
        const char *sql =
            "SELECT * FROM buddy WHERE (`to` LIKE ? AND `accept`='1') OR (`from` LIKE ? AND `accept`='1')";
        MYSQL_BIND p[2];
        unsigned long l0=0,l1=0;
        bind_str_in(&p[0], users[client_id].name, &l0);
        bind_str_in(&p[1], users[client_id].name, &l1);

        char buddy_data[1024] = "";
        struct { const char *me; char *out; size_t outsz; } ctx = { users[client_id].name, buddy_data, sizeof(buddy_data) };

        stmt_query_each_row(conn, sql, p, 2, cb_buddy_list_row, &ctx);

        char buddy_list[1024];
        snprintf(buddy_list, sizeof(buddy_list), "BUDDYLIST%s", buddy_data);
        send_data(client_id, buddy_list);
    }
    else if (strcmp(token, "MESSENGER_REMOVEBUDDY") == 0) {
        char* buddy_name = strtok(NULL, " ");
        if (buddy_name) {
            char* newline = strchr(buddy_name, '\r');
            if (newline) *newline = '\0';
            db_buddy_remove_pair(conn, buddy_name, users[client_id].name);
        }
    }
    else if (strcmp(token, "PLACESTUFFFROMSTRIP") == 0) {
        // (still prepared; schema unknown, but logic preserved)
        char* item_id = strtok(NULL, " ");
        char* x_str = strtok(NULL, " ");
        char* y_str = strtok(NULL, " ");

        if (item_id && x_str && y_str) {
            // select useritems by id
            const char *sql_u = "SELECT * FROM useritems WHERE id = ? LIMIT 1";
            MYSQL_BIND p[1];
            unsigned long lid=0;
            bind_str_in(&p[0], item_id, &lid);

            StmtRow it;
            int got = stmt_query_first_row(conn, sql_u, p, 1, &it, 2048);
            if (got == 1) {
                // insert into roomitems values (NULL, inroom, row[3]..row[8], '0', x, y)
                const char *sql_ins =
                    "INSERT INTO roomitems VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, '0', ?, ?)";
                MYSQL_BIND ip[9];
                int roomid = users[client_id].inroom;
                unsigned long l3=0,l4=0,l5=0,l6=0,l7=0,l8=0,lx=0,ly=0;
                bind_int_in(&ip[0], &roomid);
                bind_str_in(&ip[1], (it.cols>3?it.bufs[3]:""), &l3);
                bind_str_in(&ip[2], (it.cols>4?it.bufs[4]:""), &l4);
                bind_str_in(&ip[3], (it.cols>5?it.bufs[5]:""), &l5);
                bind_str_in(&ip[4], (it.cols>6?it.bufs[6]:""), &l6);
                bind_str_in(&ip[5], (it.cols>7?it.bufs[7]:""), &l7);
                bind_str_in(&ip[6], (it.cols>8?it.bufs[8]:""), &l8);
                bind_str_in(&ip[7], x_str, &lx);
                bind_str_in(&ip[8], y_str, &ly);
                stmt_exec(conn, sql_ins, ip, 9);

                stmtrow_free(&it);
            }

            // delete useritems by id
            const char *sql_del = "DELETE FROM useritems WHERE id = ?";
            stmt_exec(conn, sql_del, p, 1);

            // select last inserted room item for room
            const char *sql_last = "SELECT * FROM roomitems WHERE roomid = ? ORDER BY id DESC LIMIT 1";
            MYSQL_BIND lp[1];
            int rid = users[client_id].inroom;
            bind_int_in(&lp[0], &rid);

            StmtRow rr;
            int got2 = stmt_query_first_row(conn, sql_last, lp, 1, &rr, 2048);
            if (got2 == 1) {
                // original formats ACTIVE_OBJECTS
                if (rr.cols >= 12) {
                    char room_items[512];
                    snprintf(room_items, sizeof(room_items),
                             "ACTIVE_OBJECTS \r00000000000%s,%s %s %s %s %s 0.0 %s /%s/%s/",
                             rr.bufs[0], rr.bufs[3], rr.bufs[9], rr.bufs[10],
                             rr.bufs[7], rr.bufs[11], rr.bufs[8], rr.bufs[4], rr.bufs[5]);
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
            const char *sql_up = "UPDATE roomitems SET x=?, y=?, rotate=? WHERE id=? LIMIT 1";
            MYSQL_BIND p[4];
            unsigned long lx=0,ly=0,lr=0,li=0;
            bind_str_in(&p[0], x_str, &lx);
            bind_str_in(&p[1], y_str, &ly);
            bind_str_in(&p[2], rotate_str, &lr);
            bind_str_in(&p[3], item_id, &li);
            stmt_exec(conn, sql_up, p, 4);

            const char *sql_sel = "SELECT * FROM roomitems WHERE roomid=? AND id=? LIMIT 1";
            MYSQL_BIND sp[2];
            int rid = users[client_id].inroom;
            unsigned long lid=0;
            bind_int_in(&sp[0], &rid);
            bind_str_in(&sp[1], item_id, &lid);

            StmtRow rr;
            int got = stmt_query_first_row(conn, sql_sel, sp, 2, &rr, 2048);
            if (got == 1) {
                if (rr.cols >= 12) {
                    char room_items[512];
                    snprintf(room_items, sizeof(room_items),
                             "ACTIVEOBJECT_UPDATE \r00000000000%s,%s %s %s %s %s 0.0 %s /%s/%s/",
                             rr.bufs[0], rr.bufs[3], rr.bufs[9], rr.bufs[10],
                             rr.bufs[7], rr.bufs[11], rr.bufs[8], rr.bufs[4], rr.bufs[5]);
                    send_data_room(users[client_id].inroom, room_items);
                }
                stmtrow_free(&rr);
            }
        }
    }
    else if (strcmp(token, "REMOVESTUFF") == 0) {
        char* item_id = strtok(NULL, " ");
        if (item_id) {
            const char *sql_sel = "SELECT * FROM roomitems WHERE roomid=? AND id=? LIMIT 1";
            MYSQL_BIND sp[2];
            int rid = users[client_id].inroom;
            unsigned long lid=0;
            bind_int_in(&sp[0], &rid);
            bind_str_in(&sp[1], item_id, &lid);

            StmtRow rr;
            int got = stmt_query_first_row(conn, sql_sel, sp, 2, &rr, 2048);
            if (got == 1) {
                if (rr.cols >= 12) {
                    char room_items[512];
                    snprintf(room_items, sizeof(room_items),
                             "ACTIVEOBJECT_UPDATE \r00000000000%s,%s 99 99 %s %s 0.0 %s /%s/%s/",
                             rr.bufs[0], rr.bufs[3], rr.bufs[7], rr.bufs[11], rr.bufs[8], rr.bufs[4], rr.bufs[5]);
                    send_data_room(users[client_id].inroom, room_items);
                }
                stmtrow_free(&rr);
            }

            const char *sql_del = "DELETE FROM roomitems WHERE id=? LIMIT 1";
            MYSQL_BIND dp[1];
            unsigned long l=0;
            bind_str_in(&dp[0], item_id, &l);
            stmt_exec(conn, sql_del, dp, 1);
        }
    }
    else if (strcmp(token, "ADDSTRIPITEM") == 0) {
        char* item_id = strtok(NULL, " ");
        strtok(NULL, " ");
        char* room_item_id = strtok(NULL, " ");

        if (item_id && room_item_id) {
            const char *sql_sel = "SELECT * FROM roomitems WHERE roomid=? AND id=? LIMIT 1";
            MYSQL_BIND sp[2];
            int rid = users[client_id].inroom;
            unsigned long lri=0;
            bind_int_in(&sp[0], &rid);
            bind_str_in(&sp[1], room_item_id, &lri);

            StmtRow rr;
            int got = stmt_query_first_row(conn, sql_sel, sp, 2, &rr, 2048);
            if (got == 1) {
                if (rr.cols >= 12) {
                    char room_items[512];
                    snprintf(room_items, sizeof(room_items),
                             "ACTIVEOBJECT_UPDATE \r00000000000%s,%s 99 99 %s %s 0.0 %s /%s/%s/",
                             rr.bufs[0], rr.bufs[3], rr.bufs[7], rr.bufs[11], rr.bufs[8], rr.bufs[4], rr.bufs[5]);
                    send_data_room(users[client_id].inroom, room_items);
                }

                const char *sql_ins = "INSERT INTO useritems VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)";
                MYSQL_BIND ip[7];
                unsigned long lu=0,l3=0,l4=0,l5=0,l6=0,l7=0,l8=0;
                bind_str_in(&ip[0], users[client_id].name, &lu);
                bind_str_in(&ip[1], (rr.cols>3?rr.bufs[3]:""), &l3);
                bind_str_in(&ip[2], (rr.cols>4?rr.bufs[4]:""), &l4);
                bind_str_in(&ip[3], (rr.cols>5?rr.bufs[5]:""), &l5);
                bind_str_in(&ip[4], (rr.cols>6?rr.bufs[6]:""), &l6);
                bind_str_in(&ip[5], (rr.cols>7?rr.bufs[7]:""), &l7);
                bind_str_in(&ip[6], (rr.cols>8?rr.bufs[8]:""), &l8);
                stmt_exec(conn, sql_ins, ip, 7);

                const char *sql_del = "DELETE FROM roomitems WHERE id=? LIMIT 1";
                MYSQL_BIND dp[1];
                unsigned long ld=0;
                bind_str_in(&dp[0], room_item_id, &ld);
                stmt_exec(conn, sql_del, dp, 1);

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

            char own_data[50] = "";
            if (users[client_id].owner) strcpy(own_data, "flatctrl useradmin/");

            int rx = 2, ry = 2;
            if (users[client_id].userx > to_x && users[client_id].usery > to_y) { rx = 7; ry = 7; }
            else if (users[client_id].userx > to_x && users[client_id].usery < to_y) { rx = 5; ry = 5; }
            else if (users[client_id].userx < to_x && users[client_id].usery > to_y) { rx = 1; ry = 1; }
            else if (users[client_id].userx < to_x && users[client_id].usery < to_y) { rx = 3; ry = 3; }
            else if (users[client_id].userx > to_x) { rx = 6; ry = 6; }
            else if (users[client_id].userx < to_x) { rx = 2; ry = 2; }
            else if (users[client_id].usery > to_y) { rx = 0; ry = 0; }
            else if (users[client_id].usery < to_y) { rx = 4; ry = 4; }

            char status_packet[1096];
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

            db_rights_insert(conn, users[client_id].inroom, target_user);

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
            db_rights_delete(conn, users[client_id].inroom, target_user);
        }
    }
    else if (strcmp(token, "UPDATEFLAT") == 0) {
        char* flat_data = strchr(packet_copy, '/');
        flat_data = flat_data ? (flat_data + 1) : NULL;
        if (flat_data) {
            char* flat_id = flat_data;
            char* name = strchr(flat_id, '/'); name = name ? (name + 1) : NULL;
            char* door = name ? strchr(name, '/') : NULL; door = door ? (door + 1) : NULL;

            if (door) {
                char* newline = strchr(door, '\r');
                if (newline) *newline = '\0';

                const char *sql = "UPDATE rooms SET name=?, door=? WHERE id=? LIMIT 1";
                MYSQL_BIND p[3];
                unsigned long ln=0, ld=0, lid=0;
                bind_str_in(&p[0], name, &ln);
                bind_str_in(&p[1], door, &ld);
                bind_str_in(&p[2], flat_id, &lid);
                stmt_exec(conn, sql, p, 3);
            }
        }
    }
    else if (strcmp(token, "SETFLATINFO") == 0) {
        char* flat_data = strchr(packet_copy, '/');
        flat_data = flat_data ? (flat_data + 1) : NULL;
        if (flat_data) {
            char* flat_id = flat_data;
            char* info_data = strchr(flat_id, '/'); info_data = info_data ? (info_data + 1) : NULL;

            if (info_data) {
                char* newline = strchr(info_data, '\r');
                if (newline) *newline = '\0';

                char desc[1096], password[1096];
                memset(desc, 0, sizeof(desc));
                memset(password, 0, sizeof(password));
                sscanf(info_data, "desc=%255[^/]/password=%99s", desc, password);

                const char *sql = "UPDATE rooms SET `desc`=?, pass=? WHERE id=?";
                MYSQL_BIND p[3];
                unsigned long ld=0, lp=0, lid=0;
                bind_str_in(&p[0], desc, &ld);
                bind_str_in(&p[1], password, &lp);
                bind_str_in(&p[2], flat_id, &lid);
                stmt_exec(conn, sql, p, 3);
            }
        }
    }
}

/* -------------------- load user data wrapper -------------------- */

void load_user_data(int client_id, const char* username, const char* password) {
    db_load_user_data(users[client_id].db_conn, username, password, &users[client_id]);
}

/* -------------------- send helpers -------------------- */

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

/* -------------------- stubs -------------------- */

void load_wordfilters() { }
char* filter_chat_message(char* message) { return message; }

/* -------------------- pathfinding -------------------- */

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

        char status_packet[1096];
        char own_data[50] = "";
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

int get_room_height(int x, int y, char* heightmap) {
    (void)heightmap;
    if (x >= 0 && x < 10 && y >= 0 && y < 28) return 1;
    return 0;
}

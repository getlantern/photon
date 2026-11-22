/* Test-only application driver for the pinned, unmodified Picoquic library. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "picoquic.h"
#include "picoquic_internal.h"
#include "picoquic_config.h"
#include "picoquic_packet_loop.h"

typedef struct {
    picoquic_cnx_t *cnx;
    FILE *output;
    uint16_t alt_port;
    int requested, opened, finished, failed;
} client_state;

static int receive(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes,
    size_t length, picoquic_call_back_event_t event, void *context, void *stream_context)
{
    client_state *s = context;
    (void)stream_context;
    if (event == picoquic_callback_ready && !s->requested) {
        static const char request[] = "GET /payload.bin\r\n";
        s->requested = 1;
        return picoquic_add_to_stream(cnx, 0, (const uint8_t *)request, sizeof(request)-1, 1);
    }
    if (event == picoquic_callback_stream_data || event == picoquic_callback_stream_fin) {
        if (stream_id != 0 || (length && fwrite(bytes, 1, length, s->output) != length)) {
            s->failed = 1;
            return -1;
        }
        if (event == picoquic_callback_stream_fin) s->finished = 1;
    } else if (event == picoquic_callback_close || event == picoquic_callback_application_close ||
        event == picoquic_callback_stateless_reset || event == picoquic_callback_stream_reset) {
        if (!s->finished) s->failed = 1;
    }
    return 0;
}

static int loop(picoquic_quic_t *quic, picoquic_packet_loop_cb_enum event,
    void *context, void *argument)
{
    client_state *s = context;
    if (event == picoquic_packet_loop_ready) {
        ((picoquic_packet_loop_options_t *)argument)->provide_alt_port = 1;
    } else if (event == picoquic_packet_loop_alt_port) {
        s->alt_port = *(uint16_t *)argument;
    } else if (event == picoquic_packet_loop_after_receive || event == picoquic_packet_loop_after_send) {
        if (s->finished || s->failed) return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
        /* The pinned demo checks CID availability for ID zero before creation
         * skips the existing zero. Wait explicitly for the intended path's CID.
         * These internal queries are read-only; packet handling stays in Picoquic. */
        if (!s->opened && s->alt_port && s->cnx->is_multipath_enabled &&
            picoquic_get_cnx_state(s->cnx) == picoquic_state_ready &&
            picoquic_obtain_stashed_cnxid(s->cnx, 1) != NULL &&
            picoquic_find_or_create_local_cnxid_list(s->cnx, 1, 0) != NULL) {
            struct sockaddr_storage local = s->cnx->path[0]->first_tuple->local_addr;
            if (local.ss_family == AF_INET6)
                ((struct sockaddr_in6 *)&local)->sin6_port = htons(s->alt_port);
            else
                ((struct sockaddr_in *)&local)->sin_port = htons(s->alt_port);
            int ret = picoquic_probe_new_path_ex(s->cnx,
                (struct sockaddr *)&s->cnx->path[0]->first_tuple->peer_addr,
                (struct sockaddr *)&local, 0, picoquic_get_quic_time(quic), 0);
            if (ret != 0) {
                fprintf(stderr, "probe path 1: %d\n", ret);
                s->failed = 1;
                return PICOQUIC_NO_ERROR_TERMINATE_PACKET_LOOP;
            }
            s->opened = 1;
        }
    }
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 5 || argc > 8) {
        fprintf(stderr, "usage: %s ROOT_CERT PORT HEX_VERSION OUTPUT_FILE [--grease-quic-bit] [--ipv6] [--desired-version=HEX]\n", argv[0]);
        return 2;
    }
    int grease = 0, ipv6 = 0;
    uint32_t desired_version = 0;
    for (int i = 5; i < argc; i++) {
        if (strcmp(argv[i], "--grease-quic-bit") == 0) grease = 1;
        else if (strcmp(argv[i], "--ipv6") == 0) ipv6 = 1;
        else if (strncmp(argv[i], "--desired-version=", 18) == 0) {
            char *end = NULL;
            unsigned long value = strtoul(argv[i] + 18, &end, 16);
            if (end == argv[i] + 18 || *end || value == 0 || value > UINT32_MAX) return 2;
            desired_version = (uint32_t)value;
        }
        else { fprintf(stderr, "unknown option: %s\n", argv[i]); return 2; }
    }
    client_state state = {0};
    picoquic_quic_config_t config;
    picoquic_config_init(&config);
    config.multipath_option = 1;
    if (picoquic_config_set_option(&config, picoquic_option_ROOT_TRUST_FILE, argv[1]) != 0) return 2;
    uint64_t now = picoquic_current_time();
    picoquic_quic_t *quic = picoquic_create_and_configure(&config, receive, &state, now, NULL);
    if (quic == NULL) return 2;
    state.output = fopen(argv[4], "wb");
    if (state.output == NULL) { picoquic_free(quic); return 2; }
    struct sockaddr_storage remote = {0};
    uint16_t port = htons((uint16_t)strtoul(argv[2], NULL, 10));
    if (ipv6) {
        struct sockaddr_in6 *v6 = (struct sockaddr_in6 *)&remote;
        v6->sin6_family = AF_INET6;
        v6->sin6_port = port;
        v6->sin6_addr = in6addr_loopback;
    } else {
        struct sockaddr_in *v4 = (struct sockaddr_in *)&remote;
        v4->sin_family = AF_INET;
        v4->sin_port = port;
        v4->sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    }
    state.cnx = picoquic_create_cnx(quic, picoquic_null_connection_id, picoquic_null_connection_id,
        (struct sockaddr *)&remote, now, (uint32_t)strtoul(argv[3], NULL, 16), "localhost", "hq-interop", 1);
    if (state.cnx != NULL && desired_version != 0) picoquic_set_desired_version(state.cnx, desired_version);
    if (state.cnx != NULL && grease) {
        picoquic_tp_t params = *picoquic_get_transport_parameters(state.cnx, 1);
        params.do_grease_quic_bit = 1;
        picoquic_set_transport_parameters(state.cnx, &params);
    }
    int ret = -1;
    if (state.cnx != NULL && picoquic_start_client_cnx(state.cnx) == 0) {
        picoquic_packet_loop_param_t param = {0};
        param.local_af = ipv6 ? AF_INET6 : AF_INET;
        param.extra_socket_required = 1;
        ret = picoquic_packet_loop_v2(quic, &param, loop, &state);
    }
    int output_error = fclose(state.output);
    int success = ret == 0 && state.finished && state.opened && !state.failed && !output_error;
    if (success && desired_version != 0 && picoquic_supported_versions[state.cnx->version_index].version != desired_version) {
        fprintf(stderr, "negotiated version mismatch\n");
        success = 0;
    }
    if (!success) fprintf(stderr, "client ret=%d finished=%d opened=%d failed=%d\n", ret, state.finished, state.opened, state.failed);
    picoquic_free(quic);
    picoquic_config_clear(&config);
    return success ? 0 : 1;
}

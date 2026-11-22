/* Test-only framing adapter around the unmodified ls-qpack encoder API.
 * All integers on stdin/stdout are unsigned big-endian. No QUIC implementation
 * code is linked here. Protocol errors and library errors terminate the process.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "lsqpack.h"
#include "lsxpack_header.h"

static void read_bytes(void *p, size_t n) { if (fread(p, 1, n, stdin) != n) exit(2); }
static void write_bytes(const void *p, size_t n) { if (fwrite(p, 1, n, stdout) != n) exit(3); }
static uint32_t read32(void) { unsigned char b[4]; read_bytes(b, 4); return (uint32_t)b[0]<<24 | (uint32_t)b[1]<<16 | (uint32_t)b[2]<<8 | b[3]; }
static uint64_t read64(void) { uint64_t hi=read32(); return hi<<32 | read32(); }
static void write32(uint32_t v) { unsigned char b[4]={(unsigned char)(v>>24),(unsigned char)(v>>16),(unsigned char)(v>>8),(unsigned char)v}; write_bytes(b,4); }
int main(int argc, char **argv) {
    enum lsqpack_enc_opts options = LSQPACK_ENC_OPT_IX_AGGR | LSQPACK_ENC_OPT_NO_DUP;
    if (argc == 2 && strcmp(argv[1], "--duplicates") == 0)
        options = LSQPACK_ENC_OPT_IX_AGGR;
    else if (argc != 1) return 1;
    struct lsqpack_enc encoder;
    unsigned char updates[65536], block[65536], prefix[32], data[16384];
    size_t initial=sizeof(prefix);
    if (lsqpack_enc_init(&encoder, NULL, 256, 256, 8, options, prefix, &initial)) return 4;
    write32((uint32_t)initial); write_bytes(prefix,initial); fflush(stdout);
    int command;
    while ((command=getchar()) != EOF) {
        if (command=='Q') { lsqpack_enc_cleanup(&encoder); return 0; }
        /* Diagnostic only: observe the pinned library's counters without
         * modifying them. These struct fields are not a portable public API. */
        if (command=='S') {
            write32(encoder.qpe_ins_count);
            write32(encoder.qpe_max_acked_id);
            write32(encoder.qpe_last_ici);
            fflush(stdout);
            continue;
        }
        if (command=='F') {
            uint32_t length=read32(); if (length>sizeof(data)) return 5;
            read_bytes(data,length);
            int result=lsqpack_enc_decoder_in(&encoder,data,length);
            unsigned char status=result ? 1 : 0; write_bytes(&status,1); fflush(stdout);
            if (result) { lsqpack_enc_cleanup(&encoder); return 6; }
        } else if (command=='E') {
            uint64_t id=read64(); uint32_t count=read32(); if (count>64) return 7;
            if (lsqpack_enc_start_header(&encoder,id,0)) return 8;
            size_t uoff=0,boff=0;
            for (uint32_t i=0;i<count;++i) {
                uint32_t name=read32(),value=read32();
                if (name>sizeof(data) || value>sizeof(data)-name) return 9;
                read_bytes(data,name+value);
                struct lsxpack_header header;
                lsxpack_header_set_offset2(&header,(char *)data,0,name,name,value);
                size_t usz=sizeof(updates)-uoff,bsz=sizeof(block)-boff;
                if (lsqpack_enc_encode(&encoder,updates+uoff,&usz,block+boff,&bsz,&header,0)!=LQES_OK) return 10;
                uoff+=usz;boff+=bsz;
            }
            enum lsqpack_enc_header_flags flags;
            ssize_t plen=lsqpack_enc_end_header(&encoder,prefix,sizeof(prefix),&flags);
            if (plen<0) return 11;
            write32((uint32_t)uoff);write32((uint32_t)(plen+boff));
            write_bytes(updates,uoff);write_bytes(prefix,(size_t)plen);write_bytes(block,boff);fflush(stdout);
        } else return 12;
    }
    lsqpack_enc_cleanup(&encoder);return 0;
}

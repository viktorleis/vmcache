#ifndef DPDK_MEMCMP_HH
#define DPDK_MEMCMP_HH

// DPDK v3
// https://patchwork.dpdk.org/project/dpdk/patch/1431979303-1346-2-git-send-email-rkerur@gmail.com/

#include <immintrin.h>
#define likely(condition) __builtin_expect(condition, 1)
#define unlikely(condition) __builtin_expect(condition, 0)

/**
 * Compare bytes between two locations. The locations must not overlap.
 *
 * @param src_1
 *   Pointer to the first source of the data.
 * @param src_2
 *   Pointer to the second source of the data.
 * @param n
 *   Number of bytes to compare.
 * @return
 *   zero if src_1 equal src_2
 *   -ve if src_1 less than src_2
 *   +ve if src_1 greater than src_2
 */
static inline int
rte_memcmp(const void *src_1, const void *src,
        size_t n) __attribute__((always_inline));

/**
 * Find the first different bit for comparison.
 */
static inline int
rte_cmpffd (uint32_t x, uint32_t y)
{
    int i;
    int pos = x ^ y;
    for (i = 0; i < 32; i++)
        if (pos & (1<<i))
            return i;
    return -1;
}

/**
 * Find the first different byte for comparison.
 */
static inline int
rte_cmpffdb (const uint8_t *x, const uint8_t *y, size_t n)
{
    size_t i;
    for (i = 0; i < n; i++)
        if (x[i] != y[i])
            return x[i] - y[i];
    return 0;
}

/**
 * Compare 16 bytes between two locations.
 * locations should not overlap.
 */
static inline int
rte_cmp16(const void *src_1, const void *src_2)
{
    __m128i xmm0, xmm1, xmm2;

    xmm0 = _mm_lddqu_si128((const __m128i *)src_1);
    xmm1 = _mm_lddqu_si128((const __m128i *)src_2);
    xmm2 = _mm_xor_si128(xmm0, xmm1);

    if (unlikely(!_mm_testz_si128(xmm2, xmm2))) {

        uint64_t mm11 = _mm_extract_epi64(xmm0, 0);
        uint64_t mm12 = _mm_extract_epi64(xmm0, 1);

        uint64_t mm21 = _mm_extract_epi64(xmm1, 0);
        uint64_t mm22 = _mm_extract_epi64(xmm1, 1);

        if (mm11 == mm21)
            return rte_cmpffdb((const uint8_t *)&mm12,
                    (const uint8_t *)&mm22, 8);
        else
            return rte_cmpffdb((const uint8_t *)&mm11,
                    (const uint8_t *)&mm21, 8);
    }

    return 0;
}

/**
 * Compare 0 to 15 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_memcmp_regular(const uint8_t *src_1u, const uint8_t *src_2u, size_t n)
{
    int ret = 1;

    /**
     * Compare less than 16 bytes
     */
    if (n & 0x08) {
        ret = (*(const uint64_t *)src_1u ==
                *(const uint64_t *)src_2u);

        if ((ret != 1))
            goto exit_8;

        n -= 0x8;
        src_1u += 0x8;
        src_2u += 0x8;
    }

    if (n & 0x04) {
        ret = (*(const uint32_t *)src_1u ==
                *(const uint32_t *)src_2u);

        if ((ret != 1))
            goto exit_4;

        n -= 0x4;
        src_1u += 0x4;
        src_2u += 0x4;
    }

    if (n & 0x02) {
        ret = (*(const uint16_t *)src_1u ==
                *(const uint16_t *)src_2u);

        if ((ret != 1))
            goto exit_2;

        n -= 0x2;
        src_1u += 0x2;
        src_2u += 0x2;
    }

    if (n & 0x01) {
        ret = (*(const uint8_t *)src_1u ==
                *(const uint8_t *)src_2u);

        if ((ret != 1))
            goto exit_1;

        n -= 0x1;
        src_1u += 0x1;
        src_2u += 0x1;
    }

    return !ret;

exit_8:
    return rte_cmpffdb(src_1u, src_2u, 8);
exit_4:
    return rte_cmpffdb(src_1u, src_2u, 4);
exit_2:
    return rte_cmpffdb(src_1u, src_2u, 2);
exit_1:
    return rte_cmpffdb(src_1u, src_2u, 1);
}

/**
 * AVX2 implementation below
 */

/**
 * Compare 32 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp32(const void *src_1, const void *src_2)
{
    const __m128i* src1 = (const __m128i*)src_1;
    const __m128i* src2 = (const __m128i*)src_2;
    const uint8_t *s1, *s2;

    __m128i mm11 = _mm_lddqu_si128(src1);
    __m128i mm12 = _mm_lddqu_si128(src1 + 1);
    __m128i mm21 = _mm_lddqu_si128(src2);
    __m128i mm22 = _mm_lddqu_si128(src2 + 1);

    __m128i mm1 = _mm_xor_si128(mm11, mm21);
    __m128i mm2 = _mm_xor_si128(mm12, mm22);
    __m128i mm = _mm_or_si128(mm1, mm2);

    if (unlikely(!_mm_testz_si128(mm, mm))) {

        /*
         * Find out which of the two 16-byte blocks
         * are different.
         */
        if (_mm_testz_si128(mm1, mm1)) {
            mm11 = mm12;
            mm21 = mm22;
            mm1 = mm2;
            s1 = (const uint8_t *)(src1 + 1);
            s2 = (const uint8_t *)(src2 + 1);
        } else {
            s1 = (const uint8_t *)src1;
            s2 = (const uint8_t *)src2;
        }

        // Produce the comparison result
        __m128i mm_cmp = _mm_cmpgt_epi8(mm11, mm21);
        __m128i mm_rcmp = _mm_cmpgt_epi8(mm21, mm11);
        mm_cmp = _mm_xor_si128(mm1, mm_cmp);
        mm_rcmp = _mm_xor_si128(mm1, mm_rcmp);

        uint32_t cmp = _mm_movemask_epi8(mm_cmp);
        uint32_t rcmp = _mm_movemask_epi8(mm_rcmp);

        int cmp_b = rte_cmpffd(cmp, rcmp);

        int ret = (cmp_b == -1) ? 0 : (s1[cmp_b] - s2[cmp_b]);
        return ret;
    }

    return 0;
}

/**
 * Compare 48 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp48(const void *src_1, const void *src_2)
{
    int ret;

    ret = rte_cmp32((const uint8_t *)src_1 + 0 * 32,
            (const uint8_t *)src_2 + 0 * 32);

    if (unlikely(ret != 0))
        return ret;

    ret = rte_cmp16((const uint8_t *)src_1 + 1 * 32,
            (const uint8_t *)src_2 + 1 * 32);
    return ret;
}

/**
 * Compare 64 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp64 (const void* src_1, const void* src_2)
{
    const __m256i* src1 = (const __m256i*)src_1;
    const __m256i* src2 = (const __m256i*)src_2;
    const uint8_t *s1, *s2;

    __m256i mm11 = _mm256_lddqu_si256(src1);
    __m256i mm12 = _mm256_lddqu_si256(src1 + 1);
    __m256i mm21 = _mm256_lddqu_si256(src2);
    __m256i mm22 = _mm256_lddqu_si256(src2 + 1);

    __m256i mm1 = _mm256_xor_si256(mm11, mm21);
    __m256i mm2 = _mm256_xor_si256(mm12, mm22);
    __m256i mm = _mm256_or_si256(mm1, mm2);

    if (unlikely(!_mm256_testz_si256(mm, mm))) {
        /*
         * Find out which of the two 32-byte blocks
         * are different.
         */
        if (_mm256_testz_si256(mm1, mm1)) {
            mm11 = mm12;
            mm21 = mm22;
            mm1 = mm2;
            s1 = (const uint8_t *)(src1 + 1);
            s2 = (const uint8_t *)(src2 + 1);
        } else {
            s1 = (const uint8_t *)src1;
            s2 = (const uint8_t *)src2;
        }

        // Produce the comparison result
        __m256i mm_cmp = _mm256_cmpgt_epi8(mm11, mm21);
        __m256i mm_rcmp = _mm256_cmpgt_epi8(mm21, mm11);
        mm_cmp = _mm256_xor_si256(mm1, mm_cmp);
        mm_rcmp = _mm256_xor_si256(mm1, mm_rcmp);

        uint32_t cmp = _mm256_movemask_epi8(mm_cmp);
        uint32_t rcmp = _mm256_movemask_epi8(mm_rcmp);

        int cmp_b = rte_cmpffd(cmp, rcmp);

        int ret = (cmp_b == -1) ? 0 : (s1[cmp_b] - s2[cmp_b]);
        return ret;
    }

    return 0;
}

/**
 * Compare 128 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp128(const void *src_1, const void *src_2)
{
    int ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 0 * 64,
            (const uint8_t *)src_2 + 0 * 64);

    if (unlikely(ret != 0))
        return ret;

    return rte_cmp64((const uint8_t *)src_1 + 1 * 64,
            (const uint8_t *)src_2 + 1 * 64);
}

/**
 * Compare 256 bytes between two locations.
 * Locations should not overlap.
 */
static inline int
rte_cmp256(const void *src_1, const void *src_2)
{
    int ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 0 * 64,
            (const uint8_t *)src_2 + 0 * 64);

    if (unlikely(ret != 0))
        return ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 1 * 64,
            (const uint8_t *)src_2 + 1 * 64);

    if (unlikely(ret != 0))
        return ret;

    ret = rte_cmp64((const uint8_t *)src_1 + 2 * 64,
            (const uint8_t *)src_2 + 2 * 64);

    if (unlikely(ret != 0))
        return ret;

    return rte_cmp64((const uint8_t *)src_1 + 3 * 64,
            (const uint8_t *)src_2 + 3 * 64);
}

/**
 * Compare bytes between two locations. The locations must not overlap.
 *
 * @param src_1
 *   Pointer to the first source of the data.
 * @param src_2
 *   Pointer to the second source of the data.
 * @param n
 *   Number of bytes to compare.
 * @return
 *   zero if src_1 equal src_2
 *   -ve if src_1 less than src_2
 *   +ve if src_1 greater than src_2
 */
inline int rte_memcmp(const void *_src_1, const void *_src_2, size_t n)
{
    const uint8_t *src_1 = (const uint8_t *)_src_1;
    const uint8_t *src_2 = (const uint8_t *)_src_2;
    int ret = 0;

    if (n < 16)
        return rte_memcmp_regular(src_1, src_2, n);

    if (n <= 32) {
        ret = rte_cmp16(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 48) {
        ret = rte_cmp32(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 64) {
        ret = rte_cmp32(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp16(src_1 + 32, src_2 + 32);

        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 96) {
        ret = rte_cmp64(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp16(src_1 + 64, src_2 + 64);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

    if (n <= 128) {
        ret = rte_cmp64(src_1, src_2);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp32(src_1 + 64, src_2 + 64);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp16(src_1 + 96, src_2 + 96);
        if (unlikely(ret != 0))
            return ret;

        return rte_cmp16(src_1 - 16 + n, src_2 - 16 + n);
    }

CMP_BLOCK_LESS_THAN_512:
    if (n <= 512) {
        if (n >= 256) {
            ret = rte_cmp256(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            src_1 = src_1 + 256;
            src_2 = src_2 + 256;
            n -= 256;
        }
        if (n >= 128) {
            ret = rte_cmp128(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            src_1 = src_1 + 128;
            src_2 = src_2 + 128;
            n -= 128;
        }
        if (n >= 64) {
            n -= 64;
            ret = rte_cmp64(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            src_1 = src_1 + 64;
            src_2 = src_2 + 64;
        }
        if (n > 32) {
            ret = rte_cmp32(src_1, src_2);
            if (unlikely(ret != 0))
                return ret;
            ret = rte_cmp32(src_1 - 32 + n, src_2 - 32 + n);
            return ret;
        }
        if (n > 0)
            ret = rte_cmp32(src_1 - 32 + n, src_2 - 32 + n);

        return ret;
    }

    while (n > 512) {
        ret = rte_cmp256(src_1 + 0 * 256, src_2 + 0 * 256);
        if (unlikely(ret != 0))
            return ret;

        ret = rte_cmp256(src_1 + 1 * 256, src_2 + 1 * 256);
        if (unlikely(ret != 0))
            return ret;

        src_1 = src_1 + 512;
        src_2 = src_2 + 512;
        n -= 512;
    }
    goto CMP_BLOCK_LESS_THAN_512;
}

#endif

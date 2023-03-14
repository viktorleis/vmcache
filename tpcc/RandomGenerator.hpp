#pragma once
#include <atomic>

class MersenneTwister
{
  private:
   static const int NN = 312;
   static const int MM = 156;
   static const uint64_t MATRIX_A = 0xB5026F5AA96619E9ULL;
   static const uint64_t UM = 0xFFFFFFFF80000000ULL;
   static const uint64_t LM = 0x7FFFFFFFULL;
   uint64_t mt[NN];
   int mti;
   void init(uint64_t seed);

  public:
   MersenneTwister(uint64_t seed = 19650218ULL);
   uint64_t rnd();
};

static thread_local MersenneTwister mt_generator;

class RandomGenerator
{
  public:
   // ATTENTION: open interval [min, max)
   static u64 getRandU64(u64 min, u64 max)
   {
      u64 rand = min + (mt_generator.rnd() % (max - min));
      assert(rand < max);
      assert(rand >= min);
      return rand;
   }
   static u64 getRandU64() { return mt_generator.rnd(); }
   template <typename T>
   static inline T getRand(T min, T max)
   {
      u64 rand = getRandU64(min, max);
      return static_cast<T>(rand);
   }
   static void getRandString(u8* dst, u64 size);
};

static std::atomic<u64> mt_counter = 0;
// -------------------------------------------------------------------------------------
MersenneTwister::MersenneTwister(uint64_t seed) : mti(NN + 1)
{
   init(seed + (mt_counter++));
}
// -------------------------------------------------------------------------------------
void MersenneTwister::init(uint64_t seed)
{
   mt[0] = seed;
   for (mti = 1; mti < NN; mti++)
      mt[mti] = (6364136223846793005ULL * (mt[mti - 1] ^ (mt[mti - 1] >> 62)) + mti);
}
// -------------------------------------------------------------------------------------
uint64_t MersenneTwister::rnd()
{
   int i;
   uint64_t x;
   static uint64_t mag01[2] = {0ULL, MATRIX_A};

   if (mti >= NN) { /* generate NN words at one time */

      for (i = 0; i < NN - MM; i++) {
         x = (mt[i] & UM) | (mt[i + 1] & LM);
         mt[i] = mt[i + MM] ^ (x >> 1) ^ mag01[(int)(x & 1ULL)];
      }
      for (; i < NN - 1; i++) {
         x = (mt[i] & UM) | (mt[i + 1] & LM);
         mt[i] = mt[i + (MM - NN)] ^ (x >> 1) ^ mag01[(int)(x & 1ULL)];
      }
      x = (mt[NN - 1] & UM) | (mt[0] & LM);
      mt[NN - 1] = mt[MM - 1] ^ (x >> 1) ^ mag01[(int)(x & 1ULL)];

      mti = 0;
   }

   x = mt[mti++];

   x ^= (x >> 29) & 0x5555555555555555ULL;
   x ^= (x << 17) & 0x71D67FFFEDA60000ULL;
   x ^= (x << 37) & 0xFFF7EEE000000000ULL;
   x ^= (x >> 43);

   return x;
}
// -------------------------------------------------------------------------------------
void RandomGenerator::getRandString(u8* dst, u64 size)
{
   for (u64 t_i = 0; t_i < size; t_i++) {
      dst[t_i] = getRand(48, 123);
   }
}
// -------------------------------------------------------------------------------------

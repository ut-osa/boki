#define __FAAS_USED_IN_BINDING
#include "utils/random.h"

#include <sys/random.h>
#include <random>

namespace faas {
namespace utils {

namespace {
using RndGen = std::mt19937_64;

static constexpr size_t     kNumRndGens = 64;
static RndGen               rd_gens[kNumRndGens];
static std::atomic<size_t>  next_rd_gen{0};
static thread_local RndGen* rd_gen{nullptr};

static RndGen& GetThreadLocalRndGen() {
    if (rd_gen == nullptr) {
        size_t rd_gen_idx = next_rd_gen.fetch_add(1);
        if (rd_gen_idx >= kNumRndGens) {
            LOG(FATAL) << "Not enough statically allocated random generators, "
                          "consider enlarge kNumRndGens";
        }
        rd_gen = &rd_gens[rd_gen_idx];
        uint64_t seed;
        while (true) {
            ssize_t ret = getrandom(&seed, sizeof(uint64_t), 0);
            if (ret == -1 && errno != EINTR) {
                PLOG(FATAL) << "getrandom failed";
            }
            if (gsl::narrow_cast<size_t>(ret) == sizeof(uint64_t)) {
                break;
            }
        }
        rd_gen->seed(seed);
    }
    return *rd_gen;
}
}  // namespace

int GetRandomInt(int a, int b) {
    DCHECK_LT(a, b);
    std::uniform_int_distribution<int> distribution(a, b - 1);
    return distribution(GetThreadLocalRndGen());
}

float GetRandomFloat(float a, float b) {
    DCHECK_LE(a, b);
    std::uniform_real_distribution<float> distribution(a, b);
    return distribution(GetThreadLocalRndGen());
}

double GetRandomDouble(double a, double b) {
    DCHECK_LE(a, b);
    std::uniform_real_distribution<double> distribution(a, b);
    return distribution(GetThreadLocalRndGen());
}

}  // namespace utils
}  // namespace faas


#include <vector>
#include <memory>
#include <functional>
#include <future>

struct MoveOnly {
    std::unique_ptr<int> p;
    MoveOnly() : p(new int(5)) {}
    MoveOnly(MoveOnly&&) = default;
    MoveOnly& operator=(MoveOnly&&) = default;
    MoveOnly(const MoveOnly&) = delete;
    MoveOnly& operator=(const MoveOnly&) = delete;
};

int main() {
    std::function<std::vector<MoveOnly>()> f = []() {
        std::vector<MoveOnly> v;
        v.push_back(MoveOnly());
        return v;
    };
    
    auto future = std::async(std::launch::async, [&f]() {
        auto v = f();
        return 0;
    });
    
    future.get();
    return 0;
}


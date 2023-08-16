#include <mutex>
#include <vector>
namespace vectordb
{
    template <typename T>
    class ThreadSafeVector
    {
    public:
        void push_back(const T &value)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            data_.push_back(value);
        }

        T at(size_t index)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            return data_[index];
        }

        void set(size_t index, const T &v)
        {
            std::lock_guard<std::mutex> lock(mutex_);
            data_[index] = v;
        }

    private:
        std::vector<T> data_;
        std::mutex mutex_;
    };
}
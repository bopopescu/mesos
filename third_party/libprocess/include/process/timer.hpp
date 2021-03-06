#ifndef __PROCESS_TIMER_HPP__
#define __PROCESS_TIMER_HPP__

#include <process/dispatch.hpp>
#include <process/process.hpp>


namespace process {

class TimerProcess;

class Timer
{
public:
  Timer(double secs, const UPID& pid, internal::Dispatcher* dispatcher);

  virtual ~Timer();

  void cancel();

private:
  PID<TimerProcess> timer;
};


// Delay a dispatch to a process. Returns a timer which can attempted
// to be canceled if desired (but might be firing concurrently).

template <typename T>
Timer delay(double secs,
            const PID<T>& pid,
            void (T::*method)())
{
  std::tr1::function<void(T*)> thunk =
    std::tr1::bind(method, std::tr1::placeholders::_1);

  internal::Dispatcher* dispatcher = new internal::Dispatcher(
      std::tr1::bind(&internal::vdispatcher<T>,
                     std::tr1::placeholders::_1,
                     thunk));

  return Timer(secs, pid, dispatcher);
}


template <typename T, typename P1, typename A1>
Timer delay(double secs,
            const PID<T>& pid,
            void (T::*method)(P1),
            A1 a1)
{
  std::tr1::function<void(T*)> thunk =
    std::tr1::bind(method, std::tr1::placeholders::_1, a1);

  internal::Dispatcher* dispatcher = new internal::Dispatcher(
      std::tr1::bind(&internal::vdispatcher<T>,
                     std::tr1::placeholders::_1,
                     thunk));

  return Timer(secs, pid, dispatcher);
}


template <typename T,
          typename P1, typename P2,
          typename A1, typename A2>
Timer delay(double secs,
            const PID<T>& pid,
            void (T::*method)(P1, P2),
            A1 a1, A2 a2)
{
  std::tr1::function<void(T*)> thunk =
    std::tr1::bind(method, std::tr1::placeholders::_1, a1, a2);

  internal::Dispatcher* dispatcher = new internal::Dispatcher(
      std::tr1::bind(&internal::vdispatcher<T>,
                     std::tr1::placeholders::_1,
                     thunk));

  return Timer(secs, pid, dispatcher);
}


template <typename T,
          typename P1, typename P2, typename P3,
          typename A1, typename A2, typename A3>
Timer delay(double secs,
            const PID<T>& pid,
            void (T::*method)(P1, P2, P3),
            A1 a1, A2 a2, A3 a3)
{
  std::tr1::function<void(T*)> thunk =
    std::tr1::bind(method, std::tr1::placeholders::_1, a1, a2, a3);

  internal::Dispatcher* dispatcher = new internal::Dispatcher(
      std::tr1::bind(&internal::vdispatcher<T>,
                     std::tr1::placeholders::_1,
                     thunk));

  return Timer(secs, pid, dispatcher);
}

} // namespace process {

#endif // __PROCESS_TIMER_HPP__

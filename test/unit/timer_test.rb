require 'test_helper'

class TimerTest < Test::Unit::TestCase

  def test_runs_callback_and_stops
    witness = mock()
    witness.expects(:timer_finished)

    timer = CouchTap::Timer.new(0.1, witness, :timer_finished, 0.1)
    timer.run
    assert_equal CouchTap::Timer::RUNNING_STATUS, timer.status

    sleep 0.1
    timer.wait

    assert_equal CouchTap::Timer::IDLE_STATUS, timer.status
  end

  def test_does_not_run_if_already_running
    th = mock()
    th.expects(:abort_on_exception=)

    Thread.expects(:new).once.returns(th)

    timer = CouchTap::Timer.new(0.1, mock(), :timer_finished, 0.1)
    timer.run
    timer.run
  end
end

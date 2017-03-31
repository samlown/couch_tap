
module CouchTap
  class Timer
    IDLE_STATUS = 0
    RUNNING_STATUS = 1

    attr_reader :status

    def initialize(timeout, callback_object, callback_method, tick = 1)
      @timeout = timeout
      @callback_object = callback_object
      @callback_method = callback_method
      @status = IDLE_STATUS
      @tick = 1

      logger.debug "Timer configured with #{@timeout} s."
    end

    def run
      return unless @status == IDLE_STATUS
      @status = RUNNING_STATUS
      logger.debug "Timer starting!"
      @thread = Thread.new do
        while @status == RUNNING_STATUS
          t = Time.now
          begin
            sleep @tick
          end while (Time.now - t) < @timeout
          logger.debug "Timer firing #{@callback_object.class}##{@callback_method}"
          @callback_object.send(@callback_method)
        end
      end
      @thread.abort_on_exception = true
    end

    # TODO we could create a halt method to halt the timer immediately
    def wait
      @status = IDLE_STATUS
      logger.debug "Waiting for timer to stop..."
      @thread.join
    end

    private

    def logger
      CouchTap.logger
    end
  end
end

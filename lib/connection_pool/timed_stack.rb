##
# The TimedStack manages a pool of homogeneous connections (or any resource
# you wish to manage).  Connections are created lazily up to a given maximum
# number.

# Examples:
#
#    ts = TimedStack.new(1) { MyConnection.new }
#
#    # fetch a connection
#    conn = ts.pop
#
#    # return a connection
#    ts.push conn
#
#    conn = ts.pop
#    ts.pop timeout: 5
#    #=> raises ConnectionPool::TimeoutError after 5 seconds

class ConnectionPool::TimedStack
  attr_reader :max

  ##
  # Creates a new pool with +size+ connections that are created from the given
  # +block+.

  def initialize(size = 0, &block)
    @create_block = block
    @created = 0
    @que = []
    @max = size
    @mutex = Thread::Mutex.new
    @resource = Thread::ConditionVariable.new
    @shutdown_block = nil
  end

  ##
  # Returns +obj+ to the stack.  +options+ is ignored in TimedStack but may be
  # used by subclasses that extend TimedStack.

  def push(obj, options = {})
    @mutex.synchronize do
      if @shutdown_block
        @created -= 1 unless @created == 0
        @shutdown_block.call(obj)
      else
        store_connection obj, options
      end

      @resource.broadcast
    end
  end
  alias_method :<<, :push

  ##
  # Retrieves a connection from the stack.  If a connection is available it is
  # immediately returned.  If no connection is available within the given
  # timeout a ConnectionPool::TimeoutError is raised.
  #
  # +:timeout+ is the only checked entry in +options+ and is preferred over
  # the +timeout+ argument (which will be removed in a future release).  Other
  # options may be used by subclasses that extend TimedStack.

  def pop(timeout = 0.5, options = {})
    options, timeout = timeout, 0.5 if Hash === timeout
    timeout = options.fetch :timeout, timeout

    deadline = current_time + timeout
    loop do
      create_new = @mutex.synchronize do
        # Try taking an existing connection
        raise ConnectionPool::PoolShuttingDownError if @shutdown_block
        return fetch_connection(options) if connection_stored?(options)

        # Check if we can make a new connection and if so, optimistically increment the
        # connection count to lock that connection slot
        unless @created == @max
          @created += 1
          true
        end
      end

      # If we've decided to make a new connection
      if create_new
          begin
              # Create a new connection without the mutex and yield it
              return @create_block.call
          rescue StandardError
              # Something went wrong while making a connection; revert our optimistic increment to the
              # connection counter
              @mutex.synchronize { @created -= 1 }

              # Then re-raise so that the caller can see the connection creation error
              raise
          end
      end
      # Wait for a connection to be returned to the pool
      @mutex.synchronize do
        to_wait = deadline - current_time
        raise ConnectionPool::TimeoutError, "Waited #{timeout} sec, #{length}/#{@max} available" if to_wait <= 0
        @resource.wait(@mutex, to_wait)
      end

    end
  end

  ##
  # Shuts down the TimedStack by passing each connection to +block+ and then
  # removing it from the pool. Attempting to checkout a connection after
  # shutdown will raise +ConnectionPool::PoolShuttingDownError+ unless
  # +:reload+ is +true+.

  def shutdown(reload: false, &block)
    raise ArgumentError, "shutdown must receive a block" unless block

    @mutex.synchronize do
      @shutdown_block = block
      @resource.broadcast

      shutdown_connections
      @shutdown_block = nil if reload
    end
  end

  ##
  # Reaps connections that were checked in more than +idle_seconds+ ago.
  def reap(idle_seconds, &block)
    raise ArgumentError, "reap must receive a block" unless block
    raise ArgumentError, "idle_seconds must be a number" unless idle_seconds.is_a?(Numeric)
    raise ConnectionPool::PoolShuttingDownError if @shutdown_block

    idle.times do
      conn =
        @mutex.synchronize do
          raise ConnectionPool::PoolShuttingDownError if @shutdown_block

          reserve_idle_connection(idle_seconds)
        end
      break unless conn

      block.call(conn)
    end
  end

  ##
  # Returns +true+ if there are no available connections.

  def empty?
    (@created - @que.length) >= @max
  end

  ##
  # The number of connections available on the stack.

  def length
    @max - @created + @que.length
  end

  ##
  # The number of connections created and available on the stack.
  def idle
    @que.length
  end

  private

  def current_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must returns true if a connection is available on the stack.

  def connection_stored?(options = nil)
    !@que.empty?
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must return a connection from the stack.

  def fetch_connection(options = nil)
    @que.pop&.first
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must shut down all connections on the stack.

  def shutdown_connections(options = nil)
    while connection_stored?(options)
      conn = fetch_connection(options)
      @created -= 1 unless @created == 0
      @shutdown_block.call(conn)
    end
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method returns the oldest idle connection if it has been idle for more than idle_seconds.
  # This requires that the stack is kept in order of checked in time (oldest first).

  def reserve_idle_connection(idle_seconds)
    return unless idle_connections?(idle_seconds)

    @created -= 1 unless @created == 0

    @que.shift.first
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # Returns true if the first connection in the stack has been idle for more than idle_seconds

  def idle_connections?(idle_seconds)
    connection_stored? && (current_time - @que.first.last > idle_seconds)
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must return +obj+ to the stack.

  def store_connection(obj, options = nil)
    @que.push [obj, current_time]
  end

end

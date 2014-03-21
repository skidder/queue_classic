require 'queue_classic'
require 'queue_classic/queue'
require 'queue_classic/conn'

module QC
  class Worker

    attr_accessor :queues, :running
    # In the case no arguments are passed to the initializer,
    # the defaults are pulled from the environment variables.
    def initialize(args={})
      @fork_worker = args[:fork_worker] || QC::FORK_WORKER
      name = args[:q_name] || QC::QUEUE
      names = args[:q_names] || QC::QUEUES
      names << name unless names.include?(name)
      @queues = [ QC::Queue.new(name, args[:top_bound]) ]
      log(args.merge(:at => "worker_initialized"))
      @running = true
    end

    def running?
      @running
    end

    # Start a loop and work jobs indefinitely.
    # Call this method to start the worker.
    # This is the easiest way to start working jobs.
    def start
      while running?
        @fork_worker ? fork_and_work : work
      end
    end

    # Call this method to stop the worker.
    # The worker may not stop immediately if the worker
    # is sleeping.
    def stop
      @running = false
    end

    # This method will tell the ruby process to FORK.
    # Define setup_child to hook into the forking process.
    # Using setup_child is good for re-establishing database connections.
    def fork_and_work
      cpid = fork {setup_child; work}
      log(:at => :fork, :pid => cpid)
      Process.wait(cpid)
    end

    # This method will lock a job & process the job.
    def work
      if job = lock_job
        QC.log_yield(:at => "work", :job => job[:id]) do
          process(job)
        end
      end
    end

    # Attempt to lock a job in the queue's table.
    # Return a hash when a job is locked.
    # Caller responsible for deleting the job when finished.
    def lock_job
      log(:at => "lock_job")
      job = nil
      while running?
        job = nil
        @queues.each do |queue|
          break if job = queue.lock
        end
        break if job
        Conn.wait(@queues.map {|q| q.name})
      end
      job
    end

    # A job is processed by evaluating the target code.
    # Errors are delegated to the handle_failure method.
    # Also, this method will make the best attempt to delete the job
    # from the queue before returning.
    def process(job)
      begin
        start_heartbeat(job)
        call(job)
      rescue => e
        handle_failure(job, e)
      ensure
        stop_heartbeat
        QC::Queue.delete(job[:id])
        log(:at => "delete_job", :job => job[:id])
      end
    end

    def start_heartbeat(job)
      wid = UUIDTools::UUID.random_create
      QC.log_yield(:at => "start_heartbeat", :jid => job[:id], :wid => wid) do
        @heartbeat = Thread.new do
          loop do
            sleep(2)
            if !QC::Queue.heartbeat(wid, job[:id])
              QC.log(:at => 'heartbeat_failed', :jid => job[:id], :wid => wid)
              exit(1)
            end
          end
        end
      end
    end

    def stop_heartbeat
      QC.log_yield(:at => "stop_heartbeat") do
        @heartbeat.kill
      end
    end

    # Each job includes a method column. We will use ruby's eval
    # to grab the ruby object from memory. We send the method to
    # the object and pass the args.
    def call(job)
      args = job[:args]
      klass = eval(job[:method].split(".").first)
      message = job[:method].split(".").last
      klass.send(message, *args)
    end

    # This method will be called when an exception
    # is raised during the execution of the job.
    def handle_failure(job,e)
      log(:at => "handle_failure", :job => job, :error => e.inspect)
    end

    # This method should be overriden if
    # your worker is forking and you need to
    # re-establish database connections
    def setup_child
      log(:at => "setup_child")
    end

    def log(data)
      QC.log(data)
    end

  end
end

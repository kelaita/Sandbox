namespace :resque do
  desc "Start resque workers (process all queues, COUNT=5 by default)"
  task :start_slm_workers do
    ENV['COUNT'] ||= "5"
    ENV['QUEUE'] ||= 'slm'
      Rake::Task["resque:slm_workers"].invoke
  end

  desc "Start multiple Resque workers. Should only be used in dev mode."
  task :slm_workers do
    def log_msg(msg)
      log_file = "/tmp/resque_watchdog.log"
      f = File.open(log_file, 'a')
      f.write("[#{Time.now.to_s(:db)}] " + msg + "\n")
      f.close
    end

    def check_status(pid)
      parent_process = "ps p #{pid} | grep -v COMMAND"         # grep -v COMMAND gets rid of the header
      search_str = "Forked"
      output = IO.popen(parent_process)
      ps_line = output.gets
      output.close
      return true if ps_line !~ /#{search_str} (\d+)/       # even the parent doesn't exist - just leave

      child_process  = "ps p #{$1} | grep -v COMMAND > /dev/null"
      return true if system child_process                   # child process exists - all is well

      log_msg "Couldn't find child in Queue: #{ENV['QUEUE']} with PID: #{$1}.  Killing the parent and relaunching thread"
      return false
    end

    log_msg "Starting slm_workers, Queue: #{ENV['QUEUE']}"
    threads = []
    pids = Hash.new
    check_interval = 30
    cmd = "rake resque:work"

    ENV['COUNT'].to_i.times do
      threads << Thread.new do
        pid = fork { exec cmd }
        pids[Thread.current] = pid
        Process.waitpid(pid)
      end
    end

    threads_copy = threads
    while true
      threads.each do |thread|
        thread.join check_interval
        log_msg "Join expired: go check the status of thread: #{thread} (#{pids[thread]}, Queue: #{ENV['QUEUE']} )"
        if !check_status(pids[thread]) && sleep(1) && !check_status(pids[thread]) && sleep(1) && !check_status(pids[thread])
          threads_copy.delete(thread)
          thread.kill
          Process.kill(9, pids[thread])
          pids.delete(thread)
          threads_copy << Thread.new do
            pid = fork { exec cmd }
            pids[Thread.current] = pid
            Process.waitpid(pid)
          end
        end
      end
      threads = threads_copy
    end

  end

  desc "Start the resque monitoring webserver"
  task :web do
    system("resque-web config/initializers/resque.rb")
  end

  task :setup => :environment
end
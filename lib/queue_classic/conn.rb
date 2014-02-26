require 'thread'
require 'uri'
require 'pg'

module QC
  module Conn
    extend self
    @exec_mutex = Mutex.new

    def execute(stmt, *params)
      @exec_mutex.synchronize do
        log(:at => "exec_sql", :sql => stmt.inspect)
        begin
          params = nil if params.empty?
          r = connection.exec(stmt, params)
          result = []
          r.each {|t| result << t}
          result.length > 1 ? result : result.pop
        rescue PGError => e
          log(:error => e.inspect)
          disconnect
          raise
        end
      end
    end

    def wait(channels)
      listen_cmds = channels.map {|c| 'LISTEN "' + c + '"'}
      execute(listen_cmds.join(';'))
      wait_for_notify(WAIT_TIME)
      unlisten_cmds = channels.map {|c| 'UNLISTEN "' + c +'"'}
      execute(unlisten_cmds.join(';'))
      drain_notify
    end

    def transaction
      begin
        execute("BEGIN")
        yield
        execute("COMMIT")
      rescue Exception
        execute("ROLLBACK")
        raise
      end
    end

    def transaction_idle?
      connection.transaction_status == PGconn::PQTRANS_IDLE
    end

    def connection
      @connection ||= connect
    end

    def connection=(connection)
      unless connection.is_a? PG::Connection
        c = connection.class
        err = "connection must be an instance of PG::Connection, but was #{c}"
        raise(ArgumentError, err)
      end
      @connection = connection
    end

    def disconnect
      begin connection.finish
      ensure @connection = nil
      end
    end

    def connect
      log(:at => "establish_conn")
      conn_info = (ENV["QC_DATABASE_URL"] || ENV["DATABASE_URL"]) ? normalize_db_url(db_url) : rails_db_config
      if conn_info
        PGconn.connect(*conn_info)
      else
        raise(ArgumentError, "missing QC_DATABASE_URL or DATABASE_URL")
      end

      if conn.status != PGconn::CONNECTION_OK
        log(:error => conn.error)
      end
      conn.exec("SET application_name = '#{QC::APP_NAME}'")
      conn
    end

    def rails_db_config
      config   = Rails.configuration.database_configuration

      [
       config[Rails.env]["host"],
       config[Rails.env]["port"] || 5432,
       nil, '', #opts, tty
       config[Rails.env]["database"], # database name
       config[Rails.env]["username"],
       config[Rails.env]["password"]
      ]
    end

    def normalize_db_url(url)
      host = url.host
      host = host.gsub(/%2F/i, '/') if host

      [
       host, # host or percent-encoded socket path
       url.port || 5432,
       nil, '', #opts, tty
       url.path.gsub("/",""), # database name
       url.user,
       url.password
      ]
    end

    def db_url
      return @db_url if @db_url
      url = ENV["QC_DATABASE_URL"] ||
            ENV["DATABASE_URL"]
      @db_url = URI.parse(url)
    end

    private

    def log(msg)
      QC.log(msg)
    end

    def wait_for_notify(t)
      Array.new.tap do |msgs|
        connection.wait_for_notify(t) {|event, pid, msg| msgs << msg}
      end
    end

    def drain_notify
      until connection.notifies.nil?
        log(:at => "drain_notifications")
      end
    end

  end
end

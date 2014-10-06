module ActiveRecord
  class LogSubscriber < ActiveSupport::LogSubscriber
    IGNORE_PAYLOAD_NAMES = ["SCHEMA", "EXPLAIN"]

    def self.runtime=(value)
      ActiveRecord::RuntimeRegistry.sql_runtime = value
    end

    def self.runtime
      ActiveRecord::RuntimeRegistry.sql_runtime ||= 0
    end

    def self.reset_runtime
      rt, self.runtime = runtime, 0
      rt
    end

    def initialize
      super
      @odd = false
    end

    def render_bind(column, value)
      if column
        if column.binary?
          # This specifically deals with the PG adapter that casts bytea columns into a Hash.
          value = value[:value] if value.is_a?(Hash)
          value = "<#{value.bytesize} bytes of binary data>"
        end
        if column.respond_to?(:pg_type) && column.pg_type && column.pg_type.encoder
          # PG adapter does type casts in C before transmission to the server
          # without allocation of type casted ruby values.
          # Thats why we encode the value and decode it back to ruby
          # representation in order to get a reasonable log output.
          value = column.pg_type.encode(value)
          value = column.pg_type.decode(value)
        end

        [column.name, value]
      else
        [nil, value]
      end
    end

    def sql(event)
      self.class.runtime += event.duration
      return unless logger.debug?

      payload = event.payload

      return if IGNORE_PAYLOAD_NAMES.include?(payload[:name])

      name  = "#{payload[:name]} (#{event.duration.round(1)}ms)"
      sql   = payload[:sql]
      binds = nil

      unless (payload[:binds] || []).empty?
        binds = "  " + payload[:binds].map { |col,v|
          render_bind(col, v)
        }.inspect
      end

      if odd?
        name = color(name, CYAN, true)
        sql  = color(sql, nil, true)
      else
        name = color(name, MAGENTA, true)
      end

      debug "  #{name}  #{sql}#{binds}"
    end

    def odd?
      @odd = !@odd
    end

    def logger
      ActiveRecord::Base.logger
    end
  end
end

ActiveRecord::LogSubscriber.attach_to :active_record

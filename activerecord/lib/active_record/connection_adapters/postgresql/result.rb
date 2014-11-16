module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      class Result < ActiveRecord::Result
        attr_accessor :pgresult
        attr_writer :column_types
        attr_writer :columns

        def initialize(conn, on_error)
          @connection   = conn
          @on_error     = on_error
          @pgresult     = nil
          @columns      = nil
          @rows         = nil
          @hash_rows    = nil
          @column_types = nil
        end

        def initialize_copy(other)
          @pgresult     = pgresult
          @columns      = columns.dup
          @rows         = other.rows.dup
          @hash_rows    = nil
          @column_types = column_types.dup
        end

        def length
          rows.length
        end

        def each
          if block_given?
            hash_rows.each { |row| yield row }
          else
            hash_rows.to_enum { rows.size }
          end
        end

        def each_pair
          return to_enum(__method__) unless block_given?

          columns = @columns
          rows.each do |row|
            yield columns, row
          end
        end

        def finish_streaming
          if @pgresult
            rows
            # Clear result queue
            @connection.get_last_result
          end
        end

        def rows
          @rows ||= begin
            if @pgresult.result_status == ::PG::PGRES_SINGLE_TUPLE
              @pgresult.stream_each_row.to_a
            else
              []
            end
          rescue => e
            @on_error.call(e)
          end
        end

        private

        def hash_rows
          @hash_rows ||= begin
            # The field_name strings are frozen when retrieved from the PG::Result object.
            # This prevents them getting duped when used as keys in ActiveRecord::Base's @attributes hash
            columns = @columns
            length = columns.length

            rows.map do |row|
              # In the past we used Hash[columns.zip(row)]
              #  though elegant, the verbose way is much more efficient
              #  both time and memory wise cause it avoids a big array allocation
              #  this method is called a lot and needs to be micro optimised
              hash = {}

              index = 0

              while index < length
                hash[columns[index]] = row[index]
                index += 1
              end
              hash
            end
          end
        end
      end
    end
  end
end

module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      class Result < ActiveRecord::Result
        attr_reader :pgresult

        def initialize(columns, pgresult, conn, column_types = {}, stream_threshold=1000)
          @connection   = conn
          @pgresult     = pgresult
          @columns      = columns
          @rows         = nil
          @hash_rows    = nil
          @column_types = column_types
          @stream_threshold = stream_threshold

          # We freeze the strings to prevent them getting duped when
          # used as keys in ActiveRecord::Base's @attributes hash
          @frozen_columns = @columns.map { |c| c.dup.freeze }
        end

#         def map(&block)
#           @pgresult.stream_each do |row|
#
#           end
#         end

        def initialize_copy(other)
          @pgresult     = pgresult
          @columns      = columns.dup
          @rows         = rows.dup
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

          columns = @frozen_columns
          rows.each do |row|
            yield columns, row
          end
        end

        def rows
          @rows ||= begin
            rows = @pgresult.stream_each_row.to_a
#            $stderr.puts rows.inspect

            while @pgresult=@connection.get_result
            end

            rows
          end
          @rows
        end

        private

        def hash_rows
          @hash_rows ||= begin
            rows.map { |row| row_to_hash_row( row ) }
          end
        end

        def row_to_hash_row(row)
          # In the past we used Hash[columns.zip(row)]
          #  though elegant, the verbose way is much more efficient
          #  both time and memory wise cause it avoids a big array allocation
          #  this method is called a lot and needs to be micro optimised
          hash = {}

          index = 0
          columns = @frozen_columns
          length = columns.length

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

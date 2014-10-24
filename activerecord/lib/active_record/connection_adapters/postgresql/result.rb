module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      class Result < ActiveRecord::Result
        attr_reader :pgresult
        
        def initialize(columns, pgresult, column_types = {})
          @pgresult     = pgresult
          @columns      = columns
          @rows         = nil
          @hash_rows    = nil
          @column_types = column_types
        end

        def length
          @pgresult.ntuples
        end

        def each(&block)
          @pgresult.each(&block)
        end

        def to_hash
          hash_rows
        end

        # Returns true if there are no records.
        def empty?
          @pgresult.ntuples == 0
        end

        def to_ary
          hash_rows
        end

        def [](idx)
          @pgresult[idx]
        end

        def last
          @pgresult[length-1]
        end

        def initialize_copy(other)
          @pgresult     = pgresult
          @columns      = columns.dup
          @rows         = rows.dup
          @hash_rows    = nil
          @column_types = column_types.dup
        end

        def rows
          @rows ||= @pgresult.values
        end

        private

        def hash_rows
          @hash_rows ||= @pgresult.to_a
        end
      end
    end
  end
end

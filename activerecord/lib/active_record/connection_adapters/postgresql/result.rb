# frozen_string_literal: true

module ActiveRecord::ConnectionAdapters::PostgreSQL
  ###
  # This class encapsulates a result returned from calling
  # {#exec_query}[rdoc-ref:ConnectionAdapters::DatabaseStatements#exec_query]
  # on the PostgreSQL connection adapter.
  class Result < ActiveRecord::Result
    include Enumerable

    if PG::Result.instance_methods.include?(:tuple)
      # Use PG::Tuple added in pg-1.1.0

      def each
        return to_enum(:each){ length } unless block_given?

        l = length
        i = 0
        while i < l
          yield @pg_result.tuple(i)
          i += 1
        end
      end

      def [](i)
        @pg_result.tuple(i)
      end

    else
      # Use a lightwight ruby implementation in place of PG::Tuple.
      # It fetches values on demand from the PG::Result object.

      class DeferredRow
        include Enumerable

        Undefined = Object.new

        def initialize(pg_result, row_num, field_map, columns)
          @pg_result = pg_result
          @row_num = row_num
          @field_map = field_map
          @columns = columns
          @cache = Array.new(@columns.length, Undefined)
        end

        def fetch(name)
          if idx = @field_map[name]
            v = @cache[idx]
            return v unless v==Undefined
            @cache[idx] = @pg_result.getvalue(@row_num, idx)
          else
            yield if block_given?
          end
        end

        def [](name)
          if idx = @field_map[name]
            v = @cache[idx]
            return v unless v==Undefined
            @cache[idx] = @pg_result.getvalue(@row_num, idx)
          else
            nil
          end
        end

        def each_key(&block)
          @columns.each(&block)
        end

        def keys
          @columns
        end

        delegate :key?, to: :@field_map

        def each_value
          return to_enum(:each_value){ @columns.length } unless block_given?

          i = 0
          while i < @columns.length
            v = @cache[i]
            v = @cache[i] = @pg_result.getvalue(@row_num, i) if v==Undefined
            yield v
            i += 1
          end

          # detach result object since all values are cached
          @pg_result = nil

          self
        end

        def values
          each_value.to_a
        end

        def each(&block)
          return to_enum(:each){ @columns.length } unless block_given?

          i = 0
          each_value do |value|
            yield @columns[i], value
            i += 1
          end
        end

        def inspect
          "#<#{self.class} #{to_h.inspect}>"
        end

        def marshal_dump
          [values, @field_map, @columns]
        end

        def marshal_load(array)
          @cache, @field_map, @columns = array
        end
      end

      def each
        return to_enum(:each){ length } unless block_given?

        fm = field_map
        l = length
        i = 0
        while i < l
          yield DeferredRow.new(@pg_result, i, fm, @columns)
          i += 1
        end
      end

      def [](i)
        DeferredRow.new(@pg_result, i, field_map, @columns)
      end

      def field_map
        @field_map ||=
          begin
            i = 0
            map = {}
            f = columns
            while i < f.length
              map[f[i]] = i
              i+=1
            end
            map
          end
      end
    end

    attr_reader :columns

    def initialize(pg_result, adapter)
      @pg_result = pg_result
      @adapter = adapter
      @field_map = nil
      @length = nil
      @columns = @pg_result.fields
    end

    def initialize_copy(other)
    end

    def column_type(name)
      if i = columns.index(name)
        ftype = @pg_result.ftype i
        fmod  = @pg_result.fmod i
        @adapter.get_oid_type ftype, fmod, name
      elsif block_given?
        yield
      else
        Type.default_value
      end
    end

    # Returns true if this result set includes the column named +name+
    def includes_column?(name)
      columns.include? name
    end

    def length
      @length ||= @pg_result.ntuples
    end

    def cast_values(type_overrides = {}) # :nodoc:
      if columns.one?
        # Separated to avoid allocating an array per row
        type = type_overrides.fetch(columns.first) { column_type(columns.first) }

        column_values(0).map { |value| type.deserialize(value) }
      else
        types = columns.map do |name|
          type_overrides.fetch(name) { column_type(name) }
        end

        @pg_result.values.map do |row|
          Array.new(row.size) { |i| types[i].deserialize(row[i]) }
        end
      end
    end

    def column_values(i)
      @pg_result.column_values(i)
    end

    def last
      return nil if length == 0
      self[length-1]
    end

    def first
      return nil if length == 0
      self[0]
    end

    # Returns true if there are no records, otherwise false.
    def empty?
      length == 0
    end

    # Returns an array of hashes representing each row record.
    #
    # This method should be avoided, since materializes the whole result set and is therefore slow.
    def to_hash
      to_a.map(&:to_h)
    end

    # Returns an array of arrays representing each row record values.
    #
    # This method should be avoided, since materializes the whole result set and is therefore slow.
    def rows
      @pg_result.values
    end
  end
end

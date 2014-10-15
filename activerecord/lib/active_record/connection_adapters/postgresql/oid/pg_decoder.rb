module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module OID # :nodoc:
        module PgDecoder # :nodoc:
          attr_reader :pg_decoder

          def initialize(options = {})
            super
            @pg_decoder = nil
          end
        end
      end
    end
  end
end

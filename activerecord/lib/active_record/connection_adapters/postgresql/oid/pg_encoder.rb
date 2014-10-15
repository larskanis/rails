module ActiveRecord
  module ConnectionAdapters
    module PostgreSQL
      module OID # :nodoc:
        module PgEncoder # :nodoc:
          attr_reader :pg_encoder

          def initialize(options = {})
            super
            @pg_encoder = nil
          end
        end
      end
    end
  end
end

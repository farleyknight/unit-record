class ActiveRecord::ConnectionAdapters::UnitRecordAdapter < ::ActiveRecord::ConnectionAdapters::AbstractAdapter
  EXCEPTION_MESSAGE = "ActiveRecord is disconnected; database access is unavailable in unit tests."

  def initialize(config = {})
    super
    @strategy = config[:strategy] || :raise
    @tables = {'schema_info' => new_table_definition(nil)}
    @schema_path = Rails.root.join("db", "schema.rb")
    @visitor = Arel::Visitors::ToSql.new self if defined?(Arel::Visitors::ToSql)
  end

  # def columns(table_name, name = nil)#:nodoc:
  #   @cached_columns[table_name.to_s] ||
  #     raise("Columns are not cached for '#{table_name}' - check schema.rb")
  # end

  def columns(table_name, name = nil)
    if @tables.size <= 1
      ActiveRecord::Migration.verbose = false
      schema_path = if Pathname(@schema_path).absolute?
                      @schema_path
                    else
                      File.join(NullDB.configuration.project_root, @schema_path)
                    end
      Kernel.load(schema_path)
    end

    if table = @tables[table_name]
      table.columns.map do |col_def|
        ActiveRecord::ConnectionAdapters::NullDBAdapter::Column.new(
          col_def.name.to_s,
          col_def.default,
          col_def.type,
          col_def.null
        )
      end
    else
      []
    end
  end

  def create_table(table_name, options = {})
    table_definition = new_table_definition(self, table_name, options.delete(:temporary), options)

    unless options[:id] == false
      table_definition.primary_key(options[:primary_key] || "id")
    end

    yield table_definition if block_given?

    @tables[table_name] = table_definition
  end

  # def create_table(table_name, options={})
  #   table_definition = ActiveRecord::ConnectionAdapters::TableDefinition.new(self)
  #   table_definition.primary_key(options[:primary_key] || "id") unless options[:id] == false
  #   yield table_definition
  #   @cached_columns[table_name.to_s] =
  #     table_definition.columns.map do |c|
  #       ActiveRecord::ConnectionAdapters::Column.new(c.name.to_s, c.default, c.sql_type, c.null)
  #     end
  # end

  def native_database_types
    # Copied from the MysqlAdapter so ColumnDefinition#sql_type will work
    {
      :primary_key => "int(11) DEFAULT NULL auto_increment PRIMARY KEY",
      :string      => { :name => "varchar", :limit => 255 },
      :text        => { :name => "text" },
      :integer     => { :name => "int", :limit => 11 },
      :float       => { :name => "float" },
      :decimal     => { :name => "decimal" },
      :datetime    => { :name => "datetime" },
      :timestamp   => { :name => "datetime" },
      :time        => { :name => "time" },
      :date        => { :name => "date" },
      :binary      => { :name => "blob" },
      :boolean     => { :name => "tinyint", :limit => 1 }
    }
  end

  def change_strategy(new_strategy, &block)
    unless [:noop, :raise].include?(new_strategy.to_sym)
      raise ArgumentError, "#{new_strategy.inspect} is not a valid strategy - valid values are :noop and :raise"
    end
    begin
      old_strategy = @strategy
      @strategy = new_strategy.to_sym
      yield
    ensure
      @strategy = old_strategy
    end
  end

  def execute(sql, name = nil)
    if sql =~ /^CREATE  INDEX/
      # Skip indexes
    else
      binding.pry
    end
  end

  def insert(sql, name = nil, pk = nil, id_value = nil, sequence_name = nil)
    raise_or_noop
  end if Rails::VERSION::MAJOR == 1

  def select_rows(sql, name = nil)
    raise_or_noop []
  end

  def rename_table(table_name, new_name)
    raise_or_noop
  end

  def change_column(table_name, column_name, type, options = {})
    raise_or_noop
  end

  def change_column_default(table_name, column_name, default)
    raise_or_noop
  end

  def rename_column(table_name, column_name, new_column_name)
    raise_or_noop
  end

  def tables
    @tables.keys.map(&:to_s)
  end

  protected

  def raise_or_noop(noop_return_value = nil)
    @strategy == :raise ? raise(EXCEPTION_MESSAGE) : noop_return_value
  end

  def select(statement, name = nil, binds = [])
    raise_or_noop []
  end

  TableDefinition = ActiveRecord::ConnectionAdapters::TableDefinition

  def new_table_definition(adapter = nil, table_name = nil, is_temporary = nil, options = {})
    case ::ActiveRecord::VERSION::MAJOR
    when 4
      TableDefinition.new(native_database_types, table_name, is_temporary, options)
    when 2,3
      TableDefinition.new(adapter)
    else
      raise "Unsupported ActiveRecord version #{::ActiveRecord::VERSION::STRING}"
    end
  end
end

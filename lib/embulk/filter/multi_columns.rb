Embulk::JavaPlugin.register_filter(
  "multi_columns", "org.embulk.filter.multi_columns.MultiColumnsFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))

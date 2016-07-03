Embulk::JavaPlugin.register_filter(
  "concat", "org.embulk.filter.concat.ConcatFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))

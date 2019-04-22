Gem::Specification.new do |s|
  s.name          = "couch_tap"
  s.version       = `cat VERSION`.strip
  s.date          = File.mtime('VERSION')
  s.summary       = "Listen to a CouchDB changes feed and create rows in a relational database in real-time."
  s.description   = "Couch Tap provides a DSL that allows complex CouchDB documents to be converted into rows in a RDBMS' table. The stream of events received from the CouchDB changes feed will trigger documents to be fed into a matching filter block and saved in the database."
  s.authors       = ["Sam Lown"]
  s.email         = 'me@samlown.com'

  s.required_ruby_version = '>= 2.0.0'

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  s.add_dependency "byebug", "~> 11.0"
  s.add_dependency "couchrest", "~> 2.0"
  s.add_dependency "httpclient", "~> 2.6"
  s.add_dependency "yajl-ruby"
  s.add_dependency "sequel", ">= 4.36.0"
  s.add_dependency "activesupport", "~> 4.0"
  s.add_dependency "dogstatsd-ruby"
  s.add_dependency "retryable"
  s.add_dependency "logging"
  s.add_development_dependency "mocha"
  s.add_development_dependency "sqlite3"
  s.add_development_dependency "test-unit"
  s.add_development_dependency "timecop"
  s.add_development_dependency "webmock"
end

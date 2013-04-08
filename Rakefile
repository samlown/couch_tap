
require 'bundler'
require 'rubygems'
require 'rake/testtask'

Bundler::GemHelper.install_tasks

Rake::TestTask.new do |t|
  t.libs << 'test'
  t.test_files = FileList.new('test/unit/**/*.rb')
end

desc "Run tests"
task :default => :test


require 'bundler'
require 'rubygems'
require 'rake/testtask'

Bundler::GemHelper.install_tasks

Rake::TestTask.new do |t|
  t.name = :unit_tests
  t.libs << 'test'
  t.test_files = FileList.new('test/**/*.rb')
end

Rake::TestTask.new do |t|
  t.name = :functional_tests
  t.libs << 'test'
  t.test_files = FileList.new('test/functional/**/*.rb')
end

desc "Run tests"
task :default => [:unit_tests, :functional_tests]

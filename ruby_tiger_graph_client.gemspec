# frozen_string_literal: true

lib = File.expand_path('../lib', __FILE__)

$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'ruby_tiger_graph_client/version'

Gem::Specification.new do |spec|
  spec.name          = 'ruby_tiger_graph_client'
  spec.version       = OcTigerGraphClient::VERSION
  spec.authors       = ['OC Developers']
  spec.summary       = 'Ruby client for tigergraph'
  spec.license       = 'MIT'
  spec.homepage      = 'https://github.com/openc/ruby_tiger_graph_client'
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.require_paths = ['lib']

  spec.add_development_dependency 'rspec'
  spec.add_development_dependency 'rubocop'
end

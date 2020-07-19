
lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "ruby_tiger_graph_client/version"

Gem::Specification.new do |spec|
  spec.name          = "ruby_tiger_graph_client"
  #rubytigergraphclient
  spec.version       = OcTigerGraphClient::VERSION
  spec.authors       = ["OC Developers"]
  spec.summary       = "Ruby client for tigergraph"
  spec.description   = "Ruby client for tigergraph"
  spec.license       = "MIT"
  
  spec.homepage      = "https://github.com/openc/ruby_tiger_graph_client"
  
  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
end

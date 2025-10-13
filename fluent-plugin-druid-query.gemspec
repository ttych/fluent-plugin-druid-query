# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name    = 'fluent-plugin-druid-query'
  spec.version = '0.1.1'
  spec.authors = ['Thomas Tych']
  spec.email   = ['thomas.tych@gmail.com']

  spec.summary       = 'fluentd plugin to query Apache Druid'
  spec.homepage      = 'https://gitlab.com/ttych/fluent-plugin-druid-query'
  spec.license       = 'Apache-2.0'

  spec.required_ruby_version = '>= 3.2.0'

  spec.metadata['rubygems_mfa_required'] = 'true'

  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) ||
        f.start_with?(*%w[test/ spec/ features/ .git .circleci appveyor
                          Gemfile])
    end
  end
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bump', '~> 0.10.0'
  spec.add_development_dependency 'bundler', '~> 2.6', '>= 2.6.9'
  spec.add_development_dependency 'byebug', '~> 11.1', '>= 11.1.3'
  spec.add_development_dependency 'mocha', '~> 2.7', '>= 2.7.1'
  spec.add_development_dependency 'rake', '~> 13.2.1'
  spec.add_development_dependency 'reek', '~> 6.4'
  spec.add_development_dependency 'rubocop', '~> 1.73', '>= 1.73.1'
  spec.add_development_dependency 'rubocop-rake', '~> 0.7.1'
  spec.add_development_dependency 'ruby-lsp', '~> 0.23', '>= 0.23.23'
  spec.add_development_dependency 'simplecov', '~> 0.22.0'
  spec.add_development_dependency 'test-unit', '~> 3.6', '>= 3.6.7'
  spec.add_development_dependency 'timecop', '~> 0.9.10'

  spec.add_dependency 'druid_client', '~> 0.1'
  spec.add_dependency 'fluentd', ['>= 0.14.10', '< 2']
end

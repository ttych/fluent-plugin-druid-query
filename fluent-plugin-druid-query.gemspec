# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name    = 'fluent-plugin-druid-query'
  spec.version = '0.1.2'
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
  spec.add_development_dependency 'bundler', '~> 4.0', '>= 4.0.13'
  spec.add_development_dependency 'byebug', '~> 13.0'
  spec.add_development_dependency 'mocha', '~> 3.1'
  spec.add_development_dependency 'rake', '~> 13.4', '>= 13.4.2'
  spec.add_development_dependency 'reek', '~> 6.5'
  spec.add_development_dependency 'rubocop', '~> 1.87'
  spec.add_development_dependency 'rubocop-rake', '~> 0.7.1'
  spec.add_development_dependency 'ruby-lsp', '~> 0.26', '>= 0.26.9'
  spec.add_development_dependency 'simplecov', '~> 0.22'
  spec.add_development_dependency 'test-unit', '~> 3.7', '>= 3.7.8'
  spec.add_development_dependency 'timecop', '~> 0.9'

  spec.add_dependency 'druid_client', '~> 0.1'
  spec.add_dependency 'fluentd', ['>= 0.14.10', '< 2']
end

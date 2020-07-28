# frozen_string_literal: true

class TigerGraphClient
  class DDL
    def initialize(client_binding)
      @client_binding = client_binding
    end

    def schema
      @schema ||= erb("schema")
    end

    def query(name)
      erb("queries", name)
    end

    def generate
      basepath = Rails.root.join("tmp", "tiger_graph", Rails.env)
      FileUtils.mkdir_p basepath

      [
        %w[schema],
        %w[queries many_hops],
      ].each do |path_elements|
        filedir = basepath.join(*path_elements[0..-2])
        FileUtils.mkdir_p filedir
        filepath = filedir.join("#{path_elements.last}.gsql")
        File.write(filepath, erb(*path_elements))
      end

      nil
    end

    private

    def erb(*path_elements)
      filename = "#{path_elements.last}.gsql.erb"
      filepath = Rails.root.join("db", "tiger_graph", *path_elements[0..-2], filename)
      ERB.new(File.read(filepath)).result(@client_binding)
    end
  end
end

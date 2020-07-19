require "ruby_tiger_graph_client/version"

# This class implements methods for CRUD operations in TigerGraph using the REST
# API detailed at https://doc.tigergraph.com/RESTPP-API-User-Guide.html
# It has no knowledge of schemas or any OpenCorporates entities or logic, and
# could be extracted into a gem
class TigerGraphClient
  # This is ugly and should possibly go somewhere else
  GOOD_CODES = %w[
    REST-0001
    REST-0003
  ].freeze

  attr_reader :scheme, :host, :port, :graph

  def initialize(config_hash)
    @scheme, @host, @port, @graph = config_hash.values_at(:scheme, :host, :port, :graph)
  end

  def upsert_vertices(vertices_data)
    data = {
      "vertices" => vertices_data,
    }
    get_body(submit(data))
  end

  def upsert_data(vertices_data, edges_data)
    data = {
      "vertices" => vertices_data,
      "edges" => edges_data,
    }
    get_body(submit(data))
  end

  def delete_vertex(id, vertex_type)
    delete_at(vertex_url(id, vertex_type))
  end

  def delete_edge(left_id, right_id, edge_type, vertex_type)
    delete_at(edge_url(left_id, right_id, edge_type, vertex_type))
  end

  def find_vertex(id, vertex_type)
    find_at(vertex_url(id, vertex_type)).first
  rescue TigerException => te
    raise te unless te.message =~ /The input vertex id '#{id}' is not a valid vertex id for vertex type = #{vertex_type}/
  end

  def find_edges(left_id, right_id, edge_type, vertex_type)
    find_at(edge_url(left_id, right_id, edge_type, vertex_type))
  end

  def all_edges_for(id, vertex_type, edge_type = nil)
    find_at(edges_url(id, vertex_type, edge_type))
  end

  def delete_all_edges_for(id, vertex_type, edge_type = nil)
    delete_at(edges_url(id, vertex_type, edge_type))
  rescue TigerException => te
    raise te unless te.message =~ /The input source_vertex_id '#{id}' is not a valid vertex id for vertex type = #{vertex_type}/
  end

  def endpoints
    get_body(http_client.get("//#{host_and_port}/endpoints"))
  end

  def version
    json = http_client.fetch(:get, "//#{host_and_port}/version").body
    data = JSON.parse json[0..-2].gsub("\n", "\\n")
    data["message"]
  end

  def statistics(secs = 60)
    get_body(http_client.get("#{scheme}://#{host_and_port}/statistics?seconds=#{secs}"))
  end

  # POST /graph/{graph_name}
  def submit(data)
    http_client.post(base_url, data.to_json)
  end

  def custom_query(query_name, params = {})
    params = params.delete_if { |_, v| v.blank? }
    query = params.to_query.gsub("%5B%5D", "") # Need to remove the [] for TigerGraph
    get_body(http_client.get("#{scheme}://#{host_and_port}/query/#{graph}/#{graph}_#{query_name}?#{query}"))
  end

  def host_and_port
    @host_and_port ||= "#{host}:#{port}"
  end

  def ddl
    DDL.new(binding)
  end

  # everything below is private

  def get_path(path)
    get_body(http_client.get("#{base_url}#{path}"))
  end

  def vertex_url(id, vertex_type)
    "#{base_url}/vertices/#{vertex_type}/#{id}"
  end

  def edge_url(left_id, right_id, rel_type, vertex_type)
    "#{base_url}/edges/#{vertex_type}/#{left_id}/#{rel_type}/#{vertex_type}/#{right_id}"
  end

  def edges_url(id, vertex_type, rel_type = nil)
    ["#{base_url}/edges/#{vertex_type}/#{id}", rel_type].compact.join("/")
  end

  def http_client
    @http_client ||= TigerHTTPClient.new
  end

  def base_url
    "#{scheme}://#{host_and_port}/graph/#{graph}"
  end

  def find_at(url)
    c = http_client.get url
    data = c.body["results"]
    return data if data
    nil
  end

  def delete_at(url)
    c = http_client.delete url
    data = c.body["results"]
    return data if data
    nil
  end

  def get_body(response)
    response.body
  end

  private(
    :base_url,
    :http_client,
    :edge_url,
    :vertex_url,
    :find_at,
    :get_body,
    :get_path,
  )

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

  class TigerHTTPClient
    def initialize
      @httpclient = HTTPClient.new
    end

    def post(url, json)
      execute :post, url, json
    end

    def get(url)
      execute :get, url
    end

    def delete(url)
      execute :delete, url
    end

    def execute(command, *args)
      Response.new(fetch(command, *args))
    end

    def fetch(command, *args)
      @httpclient.send(command, *args)
    end

    class Response
      attr_reader :body

      def initialize(raw_response)
        @body = JSON.parse(raw_response.body)
        return unless @body["code"]
        return if GOOD_CODES.include?(@body["code"])
        raise TigerException.new @body["code"], @body["message"]
      end
    end
  end
end

class TigerException < RuntimeError
  attr_reader :code

  def initialize(code, message)
    @code = code
    super(message)
  end
end

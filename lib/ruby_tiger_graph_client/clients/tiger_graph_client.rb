# frozen_string_literal: true

require_relative 'tiger_graph_client/tiger_http_client.rb'
require_relative '../exceptions/tiger_exception.rb'
require_relative 'ddl/ddl.rb'

class TigerGraphClient
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
end
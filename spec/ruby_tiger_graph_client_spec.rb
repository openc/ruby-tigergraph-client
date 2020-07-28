require "spec_helper"
require "./lib/ruby_tiger_graph_client.rb"
#mock this all

RSpec.describe TigerGraphClient, use_graph_database: true do # rubocop:disable Style/MultilineIfModifier
  let(:client) { TigerGraphClient.new({:scheme=>"http", :host=>"tigergraph", :graph=>"oc", :port=>9000}) }

  context "when upserting vertices" do
    context "when vertex doesn't exist" do
      it "creates a vertex" do
        ce = client.upsert_vertices formatted_vertices_data(:entity, [["12345", { name: "Entity 1", class: "EntityKlass", activerecord_id: 54321, jurisdiction_code: "gb" }]])
        expect(ce["results"]).to eq [
          { "accepted_edges" => 0, "accepted_vertices" => 1 },
        ]

        fe = client.find_vertex "12345", :entity
        expect(fe["attributes"]).to eq(
          "name" => "Entity 1",
          "activerecord_id" => 54321,
          "class" => "EntityKlass",
          "company_number" => "", # default value in schema
          "inactive" => "", # default value in schema
          "jurisdiction_code" => "gb",
        )
      end
    end

    context "when vertex exists" do
      before do
        client.upsert_vertices formatted_vertices_data(:entity, [["12345", { name: "Entity 1", class: "EntityKlass", activerecord_id: 54321, jurisdiction_code: "gb" }]])
      end

      it "updates a vertex" do
        ce = client.upsert_vertices formatted_vertices_data(:entity, [["12345", { name: "New Name", inactive: "T", jurisdiction_code: "" }]])
        expect(ce["results"]).to eq [
          { "accepted_edges" => 0, "accepted_vertices" => 1 },
        ]

        fe = client.find_vertex "12345", :entity
        expect(fe["attributes"]).to eq(
          "name" => "New Name",
          "activerecord_id" => 54321,
          "class" => "EntityKlass",
          "company_number" => "", # default value in schema
          "inactive" => "T",
          "jurisdiction_code" => "",
        )
      end
    end
  end

  context "when retrieving a vertex" do
    before do
      client.upsert_vertices formatted_vertices_data(:entity, [["12345", { name: "Entity 1", class: "EntityKlass", activerecord_id: 54321 }]])
    end

    it "returns given vertex" do
      fe = client.find_vertex "12345", :entity
      expect(fe["attributes"]).to eq(
        "name" => "Entity 1",
        "activerecord_id" => 54321,
        "class" => "EntityKlass",
        "company_number" => "", # default value in schema
        "inactive" => "", # default value in schema
        "jurisdiction_code" => "", # default value in schema
      )
    end

    context "when vertex doesn't exist" do
      it "returns nil" do
        expect(client.find_vertex("66666", :entity)).to be_nil
      end
    end
  end

  context "when upserting edges" do
    let(:vertices_data) do
      formatted_vertices_data(
        :entity,
        [
          ["12345", name: "Important Entity", class: "Company", activerecord_id: 54321],
          ["666", name: "Day-to-Day Entity", class: "Placeholder", activerecord_id: 54321, jurisdiction_code: "de", company_number: "55555"],
        ],
      )
    end
    let(:edges_data) do
      {
        entity: {
          "12345": {
            "shareholder_of": {
              entity: {
                "666": {
                  confidence: { value: 80 },
                  earliest_date: { value: Time.parse("2010-05-01").to_i },
                  latest_date: { value: Time.parse("2018-01-03").to_i },
                  number_of_shares: { value: 66 },
                },
              },
            },
            "has_subsidiary": {
              entity: {
                "666": {
                  confidence: { value: 60 },
                  earliest_date: { value: Time.parse("2015-10-10").to_i },

                },
              },
            },
          },
        },
      }
    end

    context "when the vertices do not exist" do
      before do
        client.upsert_data vertices_data, edges_data
      end

      it "creates the vertices" do
        expect(client.find_vertex("12345", :entity)).to eq(
          "v_id" => "12345",
          "v_type" => "entity",
          "attributes" => {
            "name" => "Important Entity",
            "activerecord_id" => 54321,
            "class" => "Company",
            "company_number" => "", # default value in schema
            "jurisdiction_code" => "", # default value in schema
            "inactive" => "" # default value in schema
          },
        )

        expect(client.find_vertex("666", :entity)).to eq(
          "v_id" => "666",
          "v_type" => "entity",
          "attributes" => {
            "name" => "Day-to-Day Entity",
            "activerecord_id" => 54321,
            "class" => "Placeholder",
            "company_number" => "55555",
            "jurisdiction_code" => "de",
            "inactive" => "" # default value in schema
          },
        )
      end

      it "creates the edges" do
        result1 = client.find_edges("12345", "666", "shareholder_of", :entity)
        expect(result1.size).to eq 1
        expect(result1.first).to eq(
          "e_type" => "shareholder_of",
          "directed" => true,
          "from_id" => "12345",
          "from_type" => "entity",
          "to_id" => "666",
          "to_type" => "entity",
          "attributes" => {
            "confidence" => 80,
            "earliest_date" => Time.parse("2010-05-01 00:00:00").to_i,
            "latest_date" => Time.parse("2018-01-03 00:00:00").to_i,
            "ownership_percentage" => 0, # default value in schema
            "number_of_shares" => 66,
          },
        )
        result2 = client.find_edges("12345", "666", "has_subsidiary", :entity)
        expect(result2.size).to eq 1
        expect(result2.first).to eq(
          "e_type" => "has_subsidiary",
          "directed" => true,
          "from_id" => "12345",
          "from_type" => "entity",
          "to_id" => "666",
          "to_type" => "entity",
          "attributes" => {
            "confidence" => 60,
            "earliest_date" => Time.parse("2015-10-10 00:00:00").to_i,
            "latest_date" => 253_281_168_000, # default value in schema
            "direct" => "", # default value in schema
            "significant" => "", # default value in schema
            "percentage_controlled" => 0 # default value in schema
          },
        )
      end

      it "creates the inverse edges" do
        result1 = client.find_edges("666", "12345", "share_issuer_to", :entity)
        expect(result1.size).to eq 1
        expect(result1.first).to eq(
          "from_type" => "entity",
          "to_type" => "entity",
          "directed" => true,
          "from_id" => "666",
          "to_id" => "12345",
          "e_type" => "share_issuer_to",
          "attributes" => {
            "confidence" => 80,
            "earliest_date" => Time.parse("2010-05-01 00:00:00").to_i,
            "latest_date" => Time.parse("2018-01-03 00:00:00").to_i,
            "ownership_percentage" => 0, # default value in schema
            "number_of_shares" => 66,
          },
        )
        result2 = client.find_edges("666", "12345", "is_subsidiary", :entity)
        expect(result2.size).to eq 1
        expect(result2.first).to eq(
          "from_type" => "entity",
          "to_type" => "entity",
          "directed" => true,
          "from_id" => "666",
          "to_id" => "12345",
          "e_type" => "is_subsidiary",
          "attributes" => {
            "confidence" => 60,
            "earliest_date" => Time.parse("2015-10-10 00:00:00").to_i,
            "latest_date" => 253_281_168_000, # default value in schema
            "direct" => "", # default value in schema
            "significant" => "", # default value in schema
            "percentage_controlled" => 0 # default value in schema
          },
        )
      end
    end

    context "when the vertices do exist" do
      let(:tweaked_vertices_data) do
        formatted_vertices_data(
          :entity,
          [
            ["12345", name: "Important Entity (new name)", class: "Company", company_number: "98765", activerecord_id: 54321],
            ["666", name: "Day-to-Day Entity", class: "Placeholder"],
          ],
        )
      end

      before do
        client.upsert_vertices vertices_data
        client.upsert_data tweaked_vertices_data, edges_data
      end

      it "updates the vertices" do
        expect(client.find_vertex("12345", :entity)).to eq(
          "v_id" => "12345",
          "v_type" => "entity",
          "attributes" => {
            "activerecord_id" => 54321,
            "name" => "Important Entity (new name)",
            "class" => "Company",
            "company_number" => "98765",
            "jurisdiction_code" => "", # default value in schema
            "inactive" => "" # default value in schema
          },
        )
      end

      it "creates the edges" do
        result1 = client.find_edges("12345", "666", "shareholder_of", :entity)
        expect(result1.size).to eq 1
        expect(result1.first).to eq(
          "e_type" => "shareholder_of",
          "directed" => true,
          "from_id" => "12345",
          "from_type" => "entity",
          "to_id" => "666",
          "to_type" => "entity",
          "attributes" => {
            "confidence" => 80,
            "earliest_date" => Time.parse("2010-05-01 00:00:00").to_i,
            "latest_date" => Time.parse("2018-01-03 00:00:00").to_i,
            "ownership_percentage" => 0, # default value in schema
            "number_of_shares" => 66,
          },
        )
        result2 = client.find_edges("12345", "666", "has_subsidiary", :entity)
        expect(result2.size).to eq 1
        expect(result2.first).to eq(
          "e_type" => "has_subsidiary",
          "directed" => true,
          "from_id" => "12345",
          "from_type" => "entity",
          "to_id" => "666",
          "to_type" => "entity",
          "attributes" => {
            "confidence" => 60,
            "earliest_date" => Time.parse("2015-10-10 00:00:00").to_i,
            "latest_date" => 253_281_168_000, # default value in schema
            "direct" => "", # default value in schema
            "significant" => "", # default value in schema
            "percentage_controlled" => 0 # default value in schema
          },
        )
      end

      it "creates the inverse edge" do
        result1 = client.find_edges("666", "12345", "share_issuer_to", :entity)
        expect(result1.size).to eq 1
        expect(result1.first).to eq(
          "from_type" => "entity",
          "to_type" => "entity",
          "directed" => true,
          "from_id" => "666",
          "to_id" => "12345",
          "e_type" => "share_issuer_to",
          "attributes" => {
            "confidence" => 80,
            "earliest_date" => Time.parse("2010-05-01 00:00:00").to_i,
            "latest_date" => Time.parse("2018-01-03 00:00:00").to_i,
            "ownership_percentage" => 0, # default value in schema
            "number_of_shares" => 66,
          },
        )
        result2 = client.find_edges("666", "12345", "is_subsidiary", :entity)
        expect(result2.size).to eq 1
        expect(result2.first).to eq(
          "from_type" => "entity",
          "to_type" => "entity",
          "directed" => true,
          "from_id" => "666",
          "to_id" => "12345",
          "e_type" => "is_subsidiary",
          "attributes" => {
            "confidence" => 60,
            "earliest_date" => Time.parse("2015-10-10 00:00:00").to_i,
            "latest_date" => 253_281_168_000, # default value in schema
            "direct" => "", # default value in schema
            "significant" => "", # default value in schema
            "percentage_controlled" => 0 # default value in schema
          },
        )
      end
    end

    context "when the edge exists" do
      let(:new_edges_data) do
        {
          entity: {
            "12345": {
              "shareholder_of": {
                entity: {
                  "666": {
                    confidence: { value: 30 },
                    earliest_date: { value: Time.parse("2010-05-01").to_i },
                    latest_date: { value: Time.parse("2018-12-12").to_i },
                    number_of_shares: { value: 120 },
                  },
                },
              },
            },
          },
        }
      end

      before do
        client.upsert_data vertices_data, edges_data
      end

      context "when the edge data is different" do
        before do
          client.upsert_data vertices_data, new_edges_data
        end

        it "updates the edge" do
          # NB This is not necessarily always desired behaviour. You could have
          # several instances of the same relationship type, from different sources
          # or for different periods, and may want to store each of these.
          result = client.find_edges("12345", "666", "shareholder_of", :entity)
          expect(result.size).to eq 1
          expect(result.first).to include(
            "e_type" => "shareholder_of",
            "directed" => true,
            "from_id" => "12345",
            "from_type" => "entity",
            "to_id" => "666",
            "to_type" => "entity",
            "attributes" => {
              "confidence" => 30,
              "earliest_date" => Time.parse("2010-05-01 00:00:00").to_i,
              "latest_date" => Time.parse("2018-12-12 00:00:00").to_i,
              "number_of_shares" => 120,
              "ownership_percentage" => 0,
            },
          )
        end

        it "updates the inverse edge" do
          result = client.find_edges("666", "12345", "share_issuer_to", :entity)
          expect(result.size).to eq 1
          expect(result.first).to eq(
            "from_type" => "entity",
            "to_type" => "entity",
            "directed" => true,
            "from_id" => "666",
            "to_id" => "12345",
            "e_type" => "share_issuer_to",
            "attributes" => {
              "confidence" => 30,
              "earliest_date" => Time.parse("2010-05-01 00:00:00").to_i,
              "latest_date" => Time.parse("2018-12-12 00:00:00").to_i,
              "number_of_shares" => 120,
              "ownership_percentage" => 0,
            },
          )
        end
      end
    end
  end

  context "when bulk-creating vertices" do
    let(:raw_vertices_data) do
      Array.new(16) { |i| [i.to_s, { name: "Vertex #{i}" }] }
    end

    it "upserts all the entities" do
      client.upsert_vertices formatted_vertices_data(:entity, raw_vertices_data)
      expect(Openc::Graph::Test.get_path("/vertices/entity")["results"].count).to eq(16)
    end
  end

  context "when creating a chain of edges" do
    let(:vertices_data1) do
      formatted_vertices_data(
        :entity,
        [
          ["jefe1", name: "Actually Important Entity", class: "Company"],
          ["boss2", name: "Important Entity", class: "Company"],
        ],
      )
    end
    let(:vertices_data2) do
      formatted_vertices_data(
        :entity,
        [
          ["boss2", name: "Important Entity", class: "Company"],
          ["minion3", name: "Day-to-Day Entity", class: "Placeholder"],
        ],
      )
    end
    let(:rel_one) do
      {
        entity: {
          "jefe1": {
            "shareholder_of": {
              entity: {
                "boss2": {
                  confidence: { value: 40 },
                },
              },
            },
          },
        },
      }
    end
    let(:rel_two) do
      {
        entity: {
          "boss2": {
            "shareholder_of": {
              entity: {
                "minion3": {
                  confidence: { value: 83 },
                },
              },
            },
          },
        },
      }
    end

    before do
      client.upsert_data(vertices_data1, rel_one)
      client.upsert_data(vertices_data2, rel_two)
    end

    it "has all the edges" do
      expect(client.all_edges_for("minion3", :entity)).to match_array(
        [
          {
            "e_type" => "share_issuer_to",
            "directed" => true,
            "from_id" => "minion3",
            "from_type" => "entity",
            "to_id" => "boss2",
            "to_type" => "entity",
            "attributes" => {
              "confidence" => 83,
              "earliest_date" => -11_670_998_400, # default value in schema
              "latest_date" => 253_281_168_000, # default value in schema
              "number_of_shares" => 0, # default value in schema
              "ownership_percentage" => 0 # default value in schema
            },
          },
        ],
      )

      expect(client.all_edges_for("boss2", :entity)).to match_array(
        [
          {
            "e_type" => "shareholder_of",
            "directed" => true,
            "from_id" => "boss2",
            "from_type" => "entity",
            "to_id" => "minion3",
            "to_type" => "entity",
            "attributes" => {
              "confidence" => 83,
              "earliest_date" => -11_670_998_400, # default value in schema
              "latest_date" => 253_281_168_000, # default value in schema
              "number_of_shares" => 0, # default value in schema
              "ownership_percentage" => 0 # default value in schema
            },
          },
          {
            "e_type" => "share_issuer_to",
            "directed" => true,
            "from_id" => "boss2",
            "from_type" => "entity",
            "to_id" => "jefe1",
            "to_type" => "entity",
            "attributes" => {
              "confidence" => 40,
              "earliest_date" => -11_670_998_400, # default value in schema
              "latest_date" => 253_281_168_000, # default value in schema
              "number_of_shares" => 0, # default value in schema
              "ownership_percentage" => 0 # default value in schema
            },
          },
        ],
      )
    end
  end

  context "when deleting a vertex" do
    before do
      client.upsert_vertices formatted_vertices_data(:entity, [["12345", { name: "Entity 1", class: "EntityKlass", activerecord_id: 54321 }]])
    end

    it "deletes the vertex" do
      deletion = client.delete_vertex("12345", :entity)
      expect(deletion).to eq(
        "v_type" => "entity",
        "deleted_vertices" => 1,
      )

      expect(client.find_vertex("12345", :entity)).to be(nil)
    end
  end

  context "when deleting an edge" do
    let(:vertices_data) do
      formatted_vertices_data(
        :entity,
        [
          ["boss123", name: "Important Entity", class: "Company"],
          ["minion5", name: "Day-to-Day Entity", class: "Placeholder"],
        ],
      )
    end
    let(:edges_data) do
      {
        entity: {
          "boss123": {
            "shareholder_of": {
              entity: {
                "minion5": {
                  confidence: { value: 40 },
                },
              },
            },
          },
        },
      }
    end

    before do
      client.upsert_data vertices_data, edges_data
    end

    it "deletes only the edges" do
      deletion = client.delete_edge "boss123", "minion5", "shareholder_of", :entity
      expect(deletion.first).to eq(
        "e_type" => "shareholder_of",
        "deleted_edges" => 1,
      )
      expect(client.find_vertex("boss123", :entity)).not_to be(nil)
      expect(client.find_vertex("minion5", :entity)).not_to be(nil)
      expect(client.find_edges("boss123", "minion5", "shareholder_of", :entity)).to be_empty
      expect(client.find_edges("minion5", "boss123", "share_issuer_to", :entity)).to be_empty
    end

    it "deletes orphaned edges" do # this test is wrong
      deletion = client.delete_vertex("minion5", :entity)
      expect(deletion).to eq(
        "v_type" => "entity",
        "deleted_vertices" => 1,
      )

      [
        %w[boss123 minion5 shareholder_of],
        %w[minion5 boss123 share_issuer_to],
      ].each do |args|
        expect { client.find_edges(args[0], args[1], args[2], :entity) }.to(raise_exception do |ex|
          expect(ex).to be_a TigerException
          expect(ex.code).to eq("601")
          expect(ex.message).to match(/The .*_vertex_id '[a-z]+[0-9]*' is not a valid vertex id for vertex type = entity./)
        end)
      end
    end
  end

  context "when deleting all edges" do
    let(:vertices_data1) do
      formatted_vertices_data(
        :entity,
        [
          ["2", name: "Second Corporation", class: "Placeholder"],
          ["1", name: "First Company", class: "Company"],
        ],
      )
    end
    let(:vertices_data2) do
      formatted_vertices_data(
        :entity,
        [
          ["3", name: "Third Organisation", class: "Placeholder"],
          ["2", name: "Second Corporation", class: "Placeholder"],
        ],
      )
    end
    let(:rel_one) do
      {
        entity: {
          "2": {
            "share_issuer_to": {
              entity: {
                "1": {
                  confidence: { value: 40 },
                },
              },
            },
          },
        },
      }
    end
    let(:rel_two) do
      {
        entity: {
          "3": {
            "shareholder_of": {
              entity: {
                "2": {
                  confidence: { value: 40 },
                },
              },
            },
          },
        },
      }
    end

    before do
      client.upsert_data vertices_data1, rel_one
      client.upsert_data vertices_data2, rel_two
    end

    it "deletes all the edges" do
      sleep 10 # without this the test works, but reporting back doesn't – it
      # says that there were no deleted edges
      deletion = client.delete_all_edges_for "2", :entity
      expect(deletion).to eq(
        [
          {
            "e_type" => "shareholder_of",
            "deleted_edges" => 0,
          },
          {
            "e_type" => "share_issuer_to",
            "deleted_edges" => 2 # from 2's perspective, it is a share_issuer to both
          },
          {
            "e_type" => "has_subsidiary",
            "deleted_edges" => 0,
          },
          {
            "e_type" => "is_subsidiary",
            "deleted_edges" => 0,
          },
          {
            "e_type" => "controls",
            "deleted_edges" => 0,
          },
          {
            "e_type" => "is_controlled_by",
            "deleted_edges" => 0,
          },
        ],
      )
      expect(client.find_vertex("2", :entity)).not_to be(nil)
      expect(client.find_vertex("1", :entity)).not_to be(nil)
      expect(client.find_vertex("2", :entity)).not_to be(nil)
      expect(client.find_vertex("3", :entity)).not_to be(nil)
      expect(client.find_edges("2", "1", "share_issuer_to", :entity)).to be_empty
      expect(client.find_edges("3", "2", "shareholder_of", :entity)).to be_empty
    end

    context "when no entity in Tigergraph" do
      it "returns nil" do
        expect(client.delete_all_edges_for("999", :entity)).to eq(nil)
      end
    end
  end

  context "when creating potentially-overlapping items" do
    let(:vertices_data1) do
      formatted_vertices_data(
        :entity,
        [
          ["2", name: "Second Corporation", class: "Placeholder"],
          ["1", name: "First Company", class: "Company"],
        ],
      )
    end
    let(:vertices_data2) do
      formatted_vertices_data(
        :entity,
        [
          ["3", name: "Third Organisation", class: "Placeholder"],
          ["2", name: "Second Corporation", class: "Placeholder"],
        ],
      )
    end
    let(:rel_one) do
      {
        entity: {
          "2": {
            "share_issuer_to": {
              entity: {
                "1": {
                  confidence: { value: 53 },
                },
              },
            },
          },
        },
      }
    end
    let(:rel_two) do
      {
        entity: {
          "3": {
            "shareholder_of": {
              entity: {
                "2": {
                  confidence: { value: 40 },
                },
              },
            },
          },
        },
      }
    end

    before do
      client.upsert_data vertices_data1, rel_one
      client.upsert_data vertices_data2, rel_two
    end

    it "upserts the expected three vertices" do
      vertices = Openc::Graph::Test.get_path("/vertices/entity")["results"]
      expect(vertices.count).to eq(3)
      expect(vertices.map { |v| v["attributes"]["name"] }).to match_array [
        "First Company", "Second Corporation", "Third Organisation"
      ]
    end

    it "upserts the expected set of edges" do
      edges = Openc::Graph::Test.get_path("/edges/entity/2/share_issuer_to/entity")["results"]
      expect(edges.count).to eq(2)
      expect(edges.map { |e| Openc::Graph::Test.simplify_edge(e) }).to match_array [
        %w[2 share_issuer_to 1],
        %w[2 share_issuer_to 3],
      ]

      edges = Openc::Graph::Test.get_path("/edges/entity/1/shareholder_of/entity")["results"]
      expect(edges.count).to eq(1)
      expect(Openc::Graph::Test.simplify_edge(edges.first)).to eq %w[1 shareholder_of 2]

      edges = Openc::Graph::Test.get_path("/edges/entity/3/shareholder_of/entity")["results"]
      expect(edges.count).to eq(1)
      expect(Openc::Graph::Test.simplify_edge(edges.first)).to eq %w[3 shareholder_of 2]
    end
  end

  context "when we go off the map, it throws exceptions" do
    specify "when we request a non-existent graph" do
      allow(client).to receive(:graph).and_return("occcc")

      expect { client.find_vertex(54321, :entity) }.to(raise_exception do |ex|
        expect(ex).to be_a TigerException
        expect(ex.code).to eq("REST-1004")
        expect(ex.message).to eq("The graph name 'occcc' parsed from the url = '/graph/occcc/vertices/entity/54321' is not found, please provide a valid graph name.")
      end)
    end

    specify "when we request a non-existent endpoint" do
      expect { Openc::Graph::Test.get_path("/no-such-endpoint") }.to(raise_exception do |ex|
        expect(ex).to be_a TigerException
        expect(ex.code).to eq("REST-1000")
        expect(ex.message).to match(/Endpoint is not found/)
      end)
    end

    specify "when we request a non-existent vertex" do
      expect(client.find_vertex(1999, :entity)).to be_nil
    end

    specify "when we request a non-existent vertex-type" do
      expect { Openc::Graph::Test.get_path("/vertices/banana/1234") }.to(raise_exception do |ex|
        expect(ex).to be_a TigerException
        expect(ex.code).to eq("REST-30000")
        expect(ex.message).to match(/The input parameter vertex_type = 'banana' is not a valid vertex type in graph/)
      end)
    end

    specify "when we request a non-existent edge-type" do
      expect { client.find_edges(3456, 7890, "bottle-washer_for", :entity) }.to(raise_exception do |ex|
        expect(ex).to be_a TigerException
        expect(ex.code).to eq("REST-30000")
        expect(ex.message).to match(/The input parameter edge_type = 'bottle-washer_for' is not a valid edge type in graph/)
      end)
    end

    specify "when the server isn't responding" do
      http_client = double(described_class::TigerHTTPClient)
      allow(described_class::TigerHTTPClient).to receive(:new).and_return(http_client)
      allow(http_client).to receive(:get).and_raise(Errno::ECONNREFUSED)

      expect { client.find_vertex(54321, :entity) }.to raise_exception Errno::ECONNREFUSED
    end
  end

  context "when making custom query" do
    let(:http_client) { double(:http_client) }
    let(:dummy_response) { double(:response, body: {}) }
    let(:expected_url) { "#{client.scheme}://#{client.host}:#{client.port}/query/#{client.graph}/#{client.graph}_my_little_query?bar=baz&foo=bar" }

    before do
      allow(TigerGraphClient::TigerHTTPClient).to receive(:new).and_return(http_client)
      allow(http_client).to receive(:get).with(expected_url).and_return(dummy_response)
    end

    it "makes get query to `query` endpoint" do
      client.custom_query(:my_little_query, bar: "baz", foo: "bar")
      expect(http_client).to have_received(:get).with(expected_url)
    end

    it "makes excludes nil values from query params" do
      client.custom_query(:my_little_query, bar: "baz", foo: "bar", foobar: nil)
      expect(http_client).to have_received(:get).with(expected_url)
    end
  end

  def formatted_vertices_data(vertex_type, raw_data)
    formatted_data = {}
    raw_data.each do |id, attribs|
      formatted_data.merge!(id => Hash[attribs.map { |k, v| [k, { value: v }] }])
    end
    { vertex_type => formatted_data }
  end
end

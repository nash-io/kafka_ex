defmodule KafkaEx.Protocol.SaslHandshake do
  alias KafkaEx.Protocol
  alias KafkaEx.Protocol.Common
  require Logger

  # the ApiVersions message can also, itself, have different api versions
  @default_this_api_version 1

  @moduledoc """
  Implementation of the Kafka SaslHandshake request and response APIs

  See: https://kafka.apache.org/protocol#The_Messages_SaslHandshake
  """

  defmodule Response do
    @moduledoc false
    defstruct error_code: nil, mechanisms: nil

    @type t :: %Response{
            error_code: atom,
            mechanisms: list(String.t())
          }
  end

  @spec create_request(String.t(), integer, binary) :: binary
  def create_request(mechanism, correlation_id, client_id) do
    encoded_mechanism = Common.encode_string(mechanism)

    Protocol.create_request(
      :sasl_handshake,
      correlation_id,
      client_id,
      @default_this_api_version
    ) <> encoded_mechanism
  end

  @spec parse_response(binary) :: Response.t()
  def parse_response(
        <<_correlation_id::32-signed, error_code::16-signed,
          mechanisms::binary>>
      ) do
    mechanisms = Common.decode_string_array(mechanisms)

    %Response{
      error_code: Protocol.error(error_code),
      mechanisms: mechanisms
    }
  end
end

defmodule KafkaEx.Protocol.SaslAuthenticate do
  alias KafkaEx.Protocol
  alias KafkaEx.Protocol.Common
  require Logger

  # the ApiVersions message can also, itself, have different api versions
  @default_this_api_version 1

  @moduledoc """
  Implementation of the Kafka SaslAuthenticate request and response APIs

  See: https://kafka.apache.org/protocol.html#The_Messages_SaslAuthenticate
  """

  defmodule Response do
    @moduledoc false
    defstruct error_code: nil,
              error_message: nil,
              auth_bytes: nil,
              session_lifetime_ms: nil

    @type t :: %Response{
            error_code: atom,
            error_message: String.t() | nil,
            auth_bytes: binary(),
            session_lifetime_ms: integer() | nil
          }
  end

  @spec create_request(String.t(), String.t(), integer, binary) :: binary
  def create_request(user, password, correlation_id, client_id) do
    auth_bytes = <<0, user::binary, 0, password::binary>>
    auth_bytes = <<byte_size(auth_bytes)::32-unsigned, auth_bytes::binary>>

    Protocol.create_request(
      :sasl_authenticate,
      correlation_id,
      client_id,
      @default_this_api_version
    ) <> auth_bytes
  end

  @spec parse_response(binary) :: Response.t()
  def parse_response(
        <<_correlation_id::32-signed, error_code::16-signed, remaining::binary>>
      ) do
    {error_message, remaining} = Common.decode_string(remaining)
    {auth_bytes, _remaining} = Common.decode_bytes(remaining)

    %Response{
      error_code: Protocol.error(error_code),
      error_message: error_message,
      auth_bytes: auth_bytes
    }
  end
end

defmodule ExOpenAI.StreamingClientErrorTest do
  @moduledoc """
  HTTPoison delivers an async response as status, then headers, then body chunks.
  A client that reports the error the moment it sees a 4xx therefore reports a
  bare number and discards the provider's explanation, which arrives next.
  """

  use ExUnit.Case, async: true

  alias ExOpenAI.StreamingClient

  defp client do
    {:ok, pid} = StreamingClient.start_link(self(), fn response -> response end)
    pid
  end

  defp send_all(pid, messages) do
    Enum.each(messages, &Process.send(pid, &1, []))
    # let the GenServer drain before assertions
    :sys.get_state(pid)
  end

  test "an error status waits for the body and reports both" do
    pid = client()

    send_all(pid, [
      %HTTPoison.AsyncStatus{code: 400},
      %HTTPoison.AsyncHeaders{headers: []},
      %HTTPoison.AsyncChunk{chunk: ~s({"error":{"message":"input too long"}})},
      %HTTPoison.AsyncEnd{}
    ])

    assert_received {:"$gen_cast", {:error, message}}
    assert message =~ "received error status code: 400"
    assert message =~ "input too long"
  end

  test "a body split across chunks is reassembled" do
    pid = client()

    send_all(pid, [
      %HTTPoison.AsyncStatus{code: 429},
      %HTTPoison.AsyncChunk{chunk: ~s({"error":{"message":"rate )},
      %HTTPoison.AsyncChunk{chunk: ~s(limit exceeded"}})},
      %HTTPoison.AsyncEnd{}
    ])

    assert_received {:"$gen_cast", {:error, message}}
    assert message =~ "rate limit exceeded"
  end

  test "an error with no body still reports the status" do
    pid = client()
    send_all(pid, [%HTTPoison.AsyncStatus{code: 502}, %HTTPoison.AsyncEnd{}])

    assert_received {:"$gen_cast", {:error, "received error status code: 502"}}
  end

  test "an error body is not parsed as stream data" do
    pid = client()

    send_all(pid, [
      %HTTPoison.AsyncStatus{code: 400},
      %HTTPoison.AsyncChunk{chunk: ~s(data: {"choices":[]}\n\n)},
      %HTTPoison.AsyncEnd{}
    ])

    refute_received {:"$gen_cast", {:data, _}}
    assert_received {:"$gen_cast", {:error, _}}
  end

  test "a successful stream is untouched" do
    pid = client()

    send_all(pid, [
      %HTTPoison.AsyncStatus{code: 200},
      %HTTPoison.AsyncChunk{chunk: ~s(data: {"id":"one"}\n\n)},
      %HTTPoison.AsyncChunk{chunk: "data: [DONE]\n\n"}
    ])

    assert_received {:"$gen_cast", {:data, %{"id" => "one"}}}
    assert_received {:"$gen_cast", :finish}
  end
end

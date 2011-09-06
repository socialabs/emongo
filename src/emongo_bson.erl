%% Copyright (c) 2010 Sean Russell
%% Permission to use, copy, or modify this file is hereby granted to everybody.
%% Do what thou wilt.
%%
%% This uses the same API as the original emongo_bson, but replaces the broken
%% emongo bson encoder with Tony Hannan's bson-erlang encoder.
%% https://github.com/TonyGen/bson-erlang
-module(emongo_bson).
-export([encode/1, decode/1]).
-compile(export_all).

%% Encode a *single* Document
-spec encode([tuple()]) -> binary().
encode([]) ->
	<<5,0,0,0,0>>;

encode(Document) ->
	bson_binary:put_document(bson:document(Document)).


%% Decode *multiple* documents
-spec decode( BsonEncodedDocs::binary() ) -> [ [tuple()] ].
decode(Bin) ->
	decode(Bin, []).
	
decode(<<>>, Acc) ->
	lists:reverse(Acc);
	
decode(Bin, Acc) ->
	{Doc, Rest} = bson_binary:get_document(Bin),
	Document = bson:fields(Doc),
	decode(Rest, [Document|Acc]).

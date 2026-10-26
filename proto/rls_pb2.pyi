import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RateLimitDescriptor(_message.Message):
    __slots__ = ("entries",)
    class Entry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[RateLimitDescriptor.Entry]
    def __init__(self, entries: _Optional[_Iterable[_Union[RateLimitDescriptor.Entry, _Mapping]]] = ...) -> None: ...

class RateLimitRequest(_message.Message):
    __slots__ = ("domain", "descriptors", "hits_addend")
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTORS_FIELD_NUMBER: _ClassVar[int]
    HITS_ADDEND_FIELD_NUMBER: _ClassVar[int]
    domain: str
    descriptors: _containers.RepeatedCompositeFieldContainer[RateLimitDescriptor]
    hits_addend: int
    def __init__(self, domain: _Optional[str] = ..., descriptors: _Optional[_Iterable[_Union[RateLimitDescriptor, _Mapping]]] = ..., hits_addend: _Optional[int] = ...) -> None: ...

class RateLimitResponse(_message.Message):
    __slots__ = ("overall_code", "statuses")
    class Code(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        UNKNOWN: _ClassVar[RateLimitResponse.Code]
        OK: _ClassVar[RateLimitResponse.Code]
        OVER_LIMIT: _ClassVar[RateLimitResponse.Code]
    UNKNOWN: RateLimitResponse.Code
    OK: RateLimitResponse.Code
    OVER_LIMIT: RateLimitResponse.Code
    class DescriptorStatus(_message.Message):
        __slots__ = ("code", "limit_remaining", "duration_until_reset")
        CODE_FIELD_NUMBER: _ClassVar[int]
        LIMIT_REMAINING_FIELD_NUMBER: _ClassVar[int]
        DURATION_UNTIL_RESET_FIELD_NUMBER: _ClassVar[int]
        code: RateLimitResponse.Code
        limit_remaining: int
        duration_until_reset: _duration_pb2.Duration
        def __init__(self, code: _Optional[_Union[RateLimitResponse.Code, str]] = ..., limit_remaining: _Optional[int] = ..., duration_until_reset: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...
    OVERALL_CODE_FIELD_NUMBER: _ClassVar[int]
    STATUSES_FIELD_NUMBER: _ClassVar[int]
    overall_code: RateLimitResponse.Code
    statuses: _containers.RepeatedCompositeFieldContainer[RateLimitResponse.DescriptorStatus]
    def __init__(self, overall_code: _Optional[_Union[RateLimitResponse.Code, str]] = ..., statuses: _Optional[_Iterable[_Union[RateLimitResponse.DescriptorStatus, _Mapping]]] = ...) -> None: ...

"""Service container for dependency injection.

This module lazily instantiates the underlying functional services
(self.processor, detectors, extractor, connector, category) from your existing
`oop` package. Keeping them here avoids tight coupling in your domain
objects and makes unit testing easier.
"""
from __future__ import annotations
from typing import Optional
from dataclasses import dataclass, field
from .processor import Processor
from .parser import Parser
from .resolver import SenderResolver, ReceiverResolver, StaffResolver, AddressResolver
from .extractor import Extractor
from .connector import Connector
from .classifier import Classifier
from .forwarder import Forwarder


@dataclass
class DefaultServices:
    _processor: Optional["Processor"] = field(default=None, init=False, repr=False)
    _parser: Optional["Parser"] = field(default=None, init=False, repr=False)
    _sender_detector: Optional["SenderResolver"] = field(default=None, init=False, repr=False)
    _receiver_detector: Optional["ReceiverResolver"] = field(default=None, init=False, repr=False)
    _staff_detector: Optional["StaffResolver"] = field(default=None, init=False, repr=False)
    _extractor: Optional["Extractor"] = field(default=None, init=False, repr=False)
    _connector: Optional["Connector"] = field(default=None, init=False, repr=False)
    _classifier: Optional["Classifier"] = field(default=None, init=False, repr=False)
    _forwarder: Optional["Forwarder"] = field(default=None, init=False, repr=False)
    _addressResolver: Optional["AddressResolver"] = field(default=None, init=False, repr=False)

    def get_processor(self):
        if self._processor is None:
            from .processor import Processor  
            self._processor = Processor()
        return self._processor
    def get_parser(self):
        if self._parser is None:
            from .parser import Parser
            self._parser = Parser()
        return self._parser
    def get_sender_detector(self):
        if self._sender_detector is None:
            from .resolver import SenderResolver
            self._sender_detector = SenderResolver()
        return self._sender_detector
    def get_receiver_detector(self):
        if self._receiver_detector is None:
            from .resolver import ReceiverResolver
            self._receiver_detector = ReceiverResolver()
        return self._receiver_detector
    def get_staff_detector(self):
        if self._staff_detector is None:
            from .resolver import StaffResolver
            self._staff_detector = StaffResolver()
        return self._staff_detector
    def get_extractor(self):
        if self._extractor is None:
            from .extractor import Extractor
            self._extractor = Extractor()
        return self._extractor
    def get_connector(self):
        if self._connector is None:
            from .connector import Connector
            self._connector = Connector()
        return self._connector
    def get_classifier(self):
        if self._classifier is None:
            from .classifier import Classifier
            self._classifier = Classifier()
        return self._classifier
    def get_forwarder(self):
        if self._forwarder is None:
            from .forwarder import Forwarder
            self._forwarder = Forwarder()
        return self._forwarder
    def get_addressResolver(self):
        if self._addressResolver is None:
            from .resolver import AddressResolver
            self._addressResolver = AddressResolver()
        return self._addressResolver

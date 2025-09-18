"""Service container for dependency injection.

This module lazily instantiates the underlying functional services
(self.processor, detectors, extractor, connector, category) from your existing
`oop` package. Keeping them here avoids tight coupling in your domain
objects and makes unit testing easier.
"""
from __future__ import annotations
from typing import Optional
import pandas as pd
from dataclasses import dataclass, field
from .processor import Processor
from .parser import Parser
from .resolver import SenderResolver, ReceiverResolver, StaffResolver, AddressResolver
from .extractor import Extractor
from .connector import Connector
from .classifier import Classifier
from .forwarder import Forwarder
from .summary import SummaryService


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
    _summary_service: Optional["SummaryService"] = field(default=None, init=False, repr=False)
        
    def get_processor(self):
        if self._processor is None:
            self._processor = Processor()
        return self._processor
    
    def get_parser(self):
        if self._parser is None:
            self._parser = Parser()
        return self._parser
    
    def get_sender_detector(self):
        if self._sender_detector is None:
            self._sender_detector = SenderResolver()
        return self._sender_detector
    
    def get_receiver_detector(self):
        if self._receiver_detector is None:
            self._receiver_detector = ReceiverResolver()
        return self._receiver_detector
    
    def get_staff_detector(self):
        if self._staff_detector is None:
            self._staff_detector = StaffResolver()
        return self._staff_detector
    
    def get_extractor(self):
        if self._extractor is None:
            self._extractor = Extractor()
        return self._extractor
    
    def get_connector(self):
        if self._connector is None:
            self._connector = Connector()
        return self._connector
    
    def get_classifier(self):
        if self._classifier is None:
            self._classifier = Classifier()
        return self._classifier
    
    def get_forwarder(self):
        if self._forwarder is None:
            self._forwarder = Forwarder()
        return self._forwarder
    
    def get_addressResolver(self):
        if self._addressResolver is None:
            self._addressResolver = AddressResolver()
        return self._addressResolver
    
    def get_summary_service(self):
        if self._summary_service is None:
            self._summary_service = SummaryService()
        return self._summary_service

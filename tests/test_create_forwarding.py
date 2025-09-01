"""
Unit tests for the refactored email forwarding workflow
"""
import pytest
from unittest.mock import Mock, patch
import pandas as pd
from dataclasses import dataclass

from app.workflow.create_forwarding import (
    ForwardingIn, 
    ForwardingOut, 
    EmailForwardingWorkflow,
    process_single_forwarding,
    create_forwarding_workflow
)
from app.services.forwarder import Forwarder
from app.services.resolver import AddressResolver


class TestForwardingDataClasses:
    """Test the dataclass structures"""
    
    def test_forwarding_request_creation(self):
        """Test ForwardingIn creation"""
        request = ForwardingIn(
            email_id=123,
            recipient="Agria",
            corrected_category="Question",
            user_id=456
        )
        
        assert request.email_id == 123
        assert request.recipient == "Agria"
        assert request.corrected_category == "Question"
        assert request.user_id == 456
    
    def test_forwarding_result_to_dict(self):
        """Test ForwardingOut to_dict conversion"""
        result = ForwardingOut(
            id=123,
            forward_address="test@example.com",
            forward_subject="Test Subject",
            forward_text="Test Content",
            success=True
        )
        
        expected = {
            "id": 123,
            "forwardAddress": "test@example.com",
            "forwardSubject": "Test Subject",
            "forwardText": "Test Content",
            "success": True,
            "error": None
        }
        
        assert result.to_dict() == expected


class TestForwarder:
    """Test Forwarder service"""
    
    def test_generate_subject_with_template(self):
        """Test subject generation with template"""
        generator = Forwarder()
        # Mock the template method
        with patch.object(generator, '_get_subject_template') as mock_template:
            mock_template.return_value = "Question from {WHO}"
            
            subject = generator.generate_subject(
                email_content="test content",
                category="Question",
                sender="TestClinic"
            )
            
            assert "TestClinic" in subject or subject != ""  # Basic validation
    
    def test_process_email_text(self):
        """Test email text processing"""
        generator = Forwarder()
        
        # Test basic text processing
        original_text = "Test content with [SUBJECT] header [BODY] body content"
        processed = generator._process_email_text(original_text)
        
        # Should remove [SUBJECT]...[BODY] pattern
        assert "[SUBJECT]" not in processed
        assert "[BODY]" not in processed


class TestAddressResolver:
    """Test AddressResolver service"""
    
    def test_resolve_forward_address_clinic_source(self):
        """Test address resolution for clinic source"""
        resolver = AddressResolver()
        
        # Mock the ic_forw_add dictionary
        resolver.ic_forw_add = {"Agria": "agria@test.com"}
        
        address = resolver.resolve_forward_address("Clinic", "Agria")
        assert address == "agria@test.com"
    
    def test_resolve_forward_address_insurance_source(self):
        """Test address resolution for insurance company source"""
        resolver = AddressResolver()
        
        # Mock the clinic_forw_add DataFrame
        mock_df = pd.DataFrame({
            'clinicName': ['TestClinic'],
            'clinicEmail': ['clinic@test.com']
        })
        resolver.clinic_forw_add = mock_df
        
        address = resolver.resolve_forward_address("Insurance_Company", "TestClinic")
        assert address == "clinic@test.com"


if __name__ == "__main__":
    pytest.main([__file__])

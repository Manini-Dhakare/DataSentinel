"""
Pydantic Domain and Record Validation Models for SchemaGuard.

Defines schemas and record-level constraints for:
- Customers (CustomerRecord)
- Orders (OrderRecord)
- Rejection Detail (ValidationFailure)
"""

import re
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, field_validator, ConfigDict

EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")


class CustomerRecord(BaseModel):
    """
    Validation model for customer master records.
    """
    model_config = ConfigDict(extra="ignore")

    customer_id: int = Field(..., description="Unique customer identifier, primary key")
    customer_name: str = Field(..., min_length=1, description="Customer full legal or business name")
    email: str = Field(..., description="Unique email address for customer notifications")
    created_at: datetime = Field(..., description="Account creation timestamp in UTC")

    @field_validator("customer_name")
    @classmethod
    def validate_name_not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("customer_name must not be empty or blank")
        return v.strip()

    @field_validator("email")
    @classmethod
    def validate_email_format(cls, v: str) -> str:
        if not v or not EMAIL_REGEX.match(v.strip()):
            raise ValueError(f"Invalid email address syntax: '{v}'")
        return v.strip().lower()


class OrderRecord(BaseModel):
    """
    Validation model for orders transactional records.
    """
    model_config = ConfigDict(extra="ignore")

    order_id: int = Field(..., description="Unique order identifier, primary key")
    customer_id: int = Field(..., description="Foreign key referencing customers.customer_id")
    order_timestamp: datetime = Field(..., description="Order creation timestamp in UTC")
    amount: Decimal = Field(..., ge=Decimal("0.00"), description="Order total amount, must be >= 0.00")
    status: str = Field(..., min_length=1, description="Current order state (e.g. PENDING, COMPLETED, CANCELLED)")
    currency: Optional[str] = Field(None, description="ISO currency code, nullable")
    sales_channel: Optional[str] = Field(None, description="Initiation sales channel, nullable")

    @field_validator("status")
    @classmethod
    def validate_status_not_blank(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("status must not be empty or blank")
        return v.strip()


class ValidationFailure(BaseModel):
    """
    Represents an individual record failure during data validation.
    """
    source_row_index: int
    record_identifier: Optional[str] = None
    category: str  # NULL_VIOLATION, DUPLICATE_KEY, INVALID_FOREIGN_KEY, RANGE_VIOLATION, MALFORMED_RECORD
    column: Optional[str] = None
    reason: str
    raw_record: Dict[str, Any]

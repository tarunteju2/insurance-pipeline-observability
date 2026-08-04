"""
FastAPI Router for Claims API v2.

Provides RESTful CRUD endpoints for claims using CQRS Command Handlers and Projections.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Depends, status
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

from src.cqrs.command_handler import ClaimCommandHandler, SubmitClaimCommand
from src.cqrs.projections import ClaimCurrentStateProjection
from src.models.claims import ClaimType

router = APIRouter(prefix="/api/v2/claims", tags=["Claims V2"])


class CreateClaimRequest(BaseModel):
    policy_number: str
    claimant_id: str
    claimant_name: str
    claim_type: str
    claim_amount: float
    date_of_loss: str
    date_filed: str
    description: str
    property_address: Optional[str] = None
    vehicle_vin: Optional[str] = None
    diagnosis_code: Optional[str] = None


@router.post("/", status_code=201)
def submit_claim(req: CreateClaimRequest) -> Dict[str, Any]:
    handler = ClaimCommandHandler()
    claim_id = f"CLM-V2-{hash(req.policy_number) % 1000000:06d}"

    try:
        ctype = ClaimType(req.claim_type.lower())
    except ValueError:
        ctype = ClaimType.AUTO

    cmd = SubmitClaimCommand(
        claim_id=claim_id,
        policy_number=req.policy_number,
        claimant_id=req.claimant_id,
        claimant_name=req.claimant_name,
        claim_type=ctype,
        claim_amount=req.claim_amount,
        date_of_loss=req.date_of_loss,
        date_filed=req.date_filed,
        description=req.description,
        property_address=req.property_address,
        vehicle_vin=req.vehicle_vin,
        diagnosis_code=req.diagnosis_code,
    )

    evt = handler.handle_submit_claim(cmd)
    return {"status": "success", "claim_id": claim_id, "version": evt.aggregate_version}


@router.get("/{claim_id}")
def get_claim(claim_id: str) -> Dict[str, Any]:
    proj = ClaimCurrentStateProjection()
    claim = proj.get_claim(claim_id)
    if not claim:
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found.")
    return claim

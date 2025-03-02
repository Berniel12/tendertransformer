/**
 * IadbAdapter.js
 * Adapter for processing Inter-American Development Bank tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class IadbAdapter extends BaseSourceAdapter {
  constructor() {
    super('iadb');
  }
  
  /**
   * Extract source ID from IADB tender
   */
  getSourceId(tender) {
    return tender.procurement_id?.toString() || tender.notice_id?.toString() || tender.id?.toString();
  }
  
  /**
   * Map IADB fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.title || tender.procurement_title,
      description: tender.description || tender.procurement_description,
      publication_date: tender.publication_date ? new Date(tender.publication_date).toISOString().split('T')[0] : null,
      deadline_date: tender.deadline_date || tender.closing_date ? new Date(tender.deadline_date || tender.closing_date).toISOString().split('T')[0] : null,
      notice_id: tender.procurement_id?.toString() || tender.notice_id?.toString(),
      reference_number: tender.reference_number || tender.procurement_id,
      organization_id: tender.borrower || tender.executing_agency || tender.client,
      country: tender.country || tender.country_name,
      status: this.mapStatus(tender.status),
      tender_type: tender.procurement_method || tender.procurement_type || "Tender",
      sector: tender.sector || tender.project_sector
    };
  }
  
  /**
   * Generate standard URL for IADB tenders
   */
  generateUrl(tender) {
    if (tender.url) {
      return tender.url;
    } else if (tender.procurement_id) {
      return `https://www.iadb.org/en/projects/procurement-notices/${tender.procurement_id}`;
    }
    return null;
  }
  
  /**
   * Map IADB specific status to standard status
   */
  mapStatus(status) {
    if (!status) return null;
    
    const statusMap = {
      "active": "Open",
      "open": "Open",
      "published": "Open",
      "closed": "Closed",
      "awarded": "Awarded",
      "cancelled": "Canceled",
      "canceled": "Canceled",
      "signed": "Awarded"
    };
    
    // Also handle Spanish terms
    const spanishStatusMap = {
      "activo": "Open",
      "abierto": "Open",
      "publicado": "Open",
      "cerrado": "Closed",
      "adjudicado": "Awarded",
      "cancelado": "Canceled",
      "firmado": "Awarded"
    };
    
    const combinedMap = {...statusMap, ...spanishStatusMap};
    
    return combinedMap[status.toLowerCase()] || status;
  }
  
  /**
   * Get IADB specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR INTER-AMERICAN DEVELOPMENT BANK DATA:
    1. For 'borrower', 'executing_agency', or 'client' field - use as the organization name.
    2. Extract country information from 'country' or 'country_name' fields.
    3. For 'procurement_method' or 'procurement_type' - map to the appropriate tender type.
    4. For 'sector' or 'project_sector' - use as category information.
    5. Handle multilingual content (Spanish, Portuguese, English) - prioritize English but preserve
       important information in other languages if needed.
    6. Spanish terms translation:
       - "activo" / "abierto" / "publicado" → "Open"
       - "cerrado" → "Closed"
       - "adjudicado" / "firmado" → "Awarded"
       - "cancelado" → "Canceled"
    7. Extract financial information from 'estimated_cost', 'contract_amount', or similar fields.
    8. Look for project information in 'project_number', 'project_name', or related fields.
    `;
  }
}

module.exports = IadbAdapter; 
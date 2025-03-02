/**
 * AfdbAdapter.js
 * Adapter for processing African Development Bank tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class AfdbAdapter extends BaseSourceAdapter {
  constructor() {
    super('afdb');
  }
  
  /**
   * Extract source ID from AfDB tender
   */
  getSourceId(tender) {
    return tender.tender_id?.toString() || tender.reference_number?.toString() || tender.id?.toString();
  }
  
  /**
   * Map AfDB fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.title || tender.tender_title,
      description: tender.description || tender.tender_description,
      publication_date: tender.publication_date ? new Date(tender.publication_date).toISOString().split('T')[0] : null,
      deadline_date: tender.deadline_date || tender.closing_date ? new Date(tender.deadline_date || tender.closing_date).toISOString().split('T')[0] : null,
      notice_id: tender.tender_id?.toString() || tender.reference_number?.toString(),
      reference_number: tender.reference_number || tender.tender_id,
      organization_id: tender.borrower || tender.organization || tender.agency,
      country: tender.country || tender.location,
      status: this.mapStatus(tender.status),
      tender_type: tender.procurement_type || tender.tender_type || "Tender",
      sector: tender.sector || tender.category
    };
  }
  
  /**
   * Generate standard URL for AfDB tenders
   */
  generateUrl(tender) {
    if (tender.url) {
      return tender.url;
    } else if (tender.tender_id) {
      return `https://www.afdb.org/en/projects-operations/procurement/tender/${tender.tender_id}`;
    }
    return null;
  }
  
  /**
   * Map AfDB specific status to standard status
   */
  mapStatus(status) {
    if (!status) return null;
    
    const statusMap = {
      "active": "Open",
      "open": "Open",
      "closed": "Closed",
      "awarded": "Awarded",
      "cancelled": "Canceled",
      "canceled": "Canceled"
    };
    
    return statusMap[status.toLowerCase()] || status;
  }
  
  /**
   * Get AfDB specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR AFRICAN DEVELOPMENT BANK DATA:
    1. For 'borrower', 'organization', or 'agency' field - use as the organization name.
    2. Extract country information from 'country' or 'location' fields.
    3. For 'procurement_type' or 'tender_type' - map to the appropriate tender type.
    4. For 'sector' or 'category' - use as category information.
    5. For 'description' or 'tender_description' - extract as much structured information as possible.
    6. Extract financial information from 'estimated_cost', 'contract_value', or similar fields.
    7. Look for project information in 'project_name', 'project_id', or related fields.
    8. Watch for multilingual entries - prioritize English but retain information in other languages if useful.
    `;
  }
}

module.exports = AfdbAdapter; 
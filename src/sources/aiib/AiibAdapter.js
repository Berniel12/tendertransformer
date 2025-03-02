/**
 * AiibAdapter.js
 * Adapter for processing Asian Infrastructure Investment Bank tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class AiibAdapter extends BaseSourceAdapter {
  constructor() {
    super('aiib');
  }
  
  /**
   * Extract source ID from AIIB tender
   */
  getSourceId(tender) {
    return tender.notice_id?.toString() || tender.tender_id?.toString() || tender.id?.toString();
  }
  
  /**
   * Map AIIB fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.title || tender.tender_title || tender.notice_title,
      description: tender.description || tender.tender_description || tender.notice_description,
      publication_date: tender.publication_date || tender.posting_date ? new Date(tender.publication_date || tender.posting_date).toISOString().split('T')[0] : null,
      deadline_date: tender.deadline_date || tender.closing_date ? new Date(tender.deadline_date || tender.closing_date).toISOString().split('T')[0] : null,
      notice_id: tender.notice_id?.toString() || tender.tender_id?.toString(),
      reference_number: tender.reference_number || tender.notice_id || tender.tender_id,
      organization_id: tender.borrower || tender.client || tender.organization,
      country: tender.country || tender.location,
      status: this.mapStatus(tender.status),
      tender_type: tender.procurement_type || tender.tender_type || "Tender",
      sector: tender.sector || tender.category
    };
  }
  
  /**
   * Generate standard URL for AIIB tenders
   */
  generateUrl(tender) {
    if (tender.url) {
      return tender.url;
    } else if (tender.notice_id) {
      return `https://www.aiib.org/en/opportunities/business/procurement/notices/${tender.notice_id}.html`;
    }
    return null;
  }
  
  /**
   * Map AIIB specific status to standard status
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
   * Get AIIB specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR ASIAN INFRASTRUCTURE INVESTMENT BANK DATA:
    1. For 'borrower', 'client', or 'organization' field - use as the organization name.
    2. Extract country information from 'country' or 'location' fields.
    3. For 'procurement_type' or 'tender_type' - map to the appropriate tender type.
    4. For 'sector' or 'category' - use as category information.
    5. For 'description', 'tender_description', or 'notice_description' - extract as much structured information as possible.
    6. Extract financial information from 'estimated_cost', 'contract_value', or similar fields.
    7. Look for project information in 'project_name', 'project_id', or related fields.
    8. Pay attention to infrastructure-specific terminology and categorize accordingly.
    `;
  }
}

module.exports = AiibAdapter; 
/**
 * UngmAdapter.js
 * Adapter for processing United Nations Global Marketplace tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class UngmAdapter extends BaseSourceAdapter {
  constructor() {
    super('ungm');
  }
  
  /**
   * Extract source ID from UNGM tender
   */
  getSourceId(tender) {
    return tender.notice_id?.toString() || tender.tender_id?.toString() || tender.reference?.toString() || tender.id?.toString();
  }
  
  /**
   * Map UNGM fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.title || tender.tender_title,
      description: tender.description || tender.tender_description,
      publication_date: tender.publication_date || tender.published_date ? new Date(tender.publication_date || tender.published_date).toISOString().split('T')[0] : null,
      deadline_date: tender.deadline_date || tender.closing_date ? new Date(tender.deadline_date || tender.closing_date).toISOString().split('T')[0] : null,
      notice_id: tender.notice_id?.toString() || tender.tender_id?.toString() || tender.reference?.toString(),
      reference_number: tender.reference_number || tender.reference || tender.notice_id,
      organization_id: tender.organization || tender.agency || tender.un_organization,
      country: tender.country || tender.delivery_country,
      status: this.mapStatus(tender.status),
      tender_type: tender.tender_type || tender.notice_type || "Tender",
      sector: tender.sector || tender.category || tender.unspsc
    };
  }
  
  /**
   * Generate standard URL for UNGM tenders
   */
  generateUrl(tender) {
    if (tender.url) {
      return tender.url;
    } else if (tender.notice_id) {
      return `https://www.ungm.org/Public/Notice/${tender.notice_id}`;
    }
    return null;
  }
  
  /**
   * Map UNGM specific status to standard status
   */
  mapStatus(status) {
    if (!status) return null;
    
    const statusMap = {
      "active": "Open",
      "open": "Open",
      "closed": "Closed",
      "awarded": "Awarded",
      "cancelled": "Canceled",
      "canceled": "Canceled",
      "completed": "Closed",
      "expired": "Closed"
    };
    
    return statusMap[status.toLowerCase()] || status;
  }
  
  /**
   * Get UNGM specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR UNITED NATIONS GLOBAL MARKETPLACE DATA:
    1. For 'organization', 'agency', or 'un_organization' field - use as the organization name.
    2. Extract country information from 'country' or 'delivery_country' fields.
    3. Pay special attention to the UN organization posting the tender (e.g., UNDP, UNICEF, WHO).
    4. For 'sector', 'category', or 'unspsc' - use as category information. UNSPSC codes are especially valuable.
    5. For UN-specific terminology in tenders:
       - "Request for Proposal (RFP)" → "Request for Proposal"
       - "Invitation to Bid (ITB)" → "Tender"
       - "Request for Quotation (RFQ)" → "Request for Quotation"
       - "Expression of Interest (EOI)" → "Expression of Interest"
    6. Extract financial information and pay attention to the currency.
    7. Look for development goals or sustainability targets mentioned in the description.
    8. Generate standard URL format using notice_id: https://www.ungm.org/Public/Notice/{notice_id}
    `;
  }
}

module.exports = UngmAdapter; 
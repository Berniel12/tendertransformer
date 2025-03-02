/**
 * AdbAdapter.js
 * Adapter for processing Asian Development Bank tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class AdbAdapter extends BaseSourceAdapter {
  constructor() {
    super('adb');
  }
  
  /**
   * Extract source ID from ADB tender
   */
  getSourceId(tender) {
    return tender.notice_id?.toString() || tender.id?.toString();
  }
  
  /**
   * Map ADB fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.title || tender.notice_title,
      description: tender.description || tender.notice_details,
      publication_date: tender.publication_date ? new Date(tender.publication_date).toISOString().split('T')[0] : null,
      deadline_date: tender.deadline_date || tender.closing_date ? new Date(tender.deadline_date || tender.closing_date).toISOString().split('T')[0] : null,
      notice_id: tender.notice_id?.toString() || tender.reference_number?.toString(),
      reference_number: tender.reference_number || tender.notice_id,
      organization_id: tender.borrower || tender.agency,
      country: tender.country,
      status: this.mapStatus(tender.status),
      tender_type: tender.procurement_type || "Tender",
      sector: tender.sector || tender.category
    };
  }
  
  /**
   * Generate standard URL for ADB tenders
   */
  generateUrl(tender) {
    if (tender.notice_id) {
      return `https://www.adb.org/projects/tenders/${tender.notice_id}`;
    }
    return null;
  }
  
  /**
   * Map ADB specific status to standard status
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
   * Get ADB specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR ASIAN DEVELOPMENT BANK DATA:
    1. For 'agency' or 'borrower' field - use as the organization name.
    2. Extract country information from the 'country' field.
    3. For 'procurement_type' - map to the appropriate tender type.
    4. For 'sector' or 'category' - use as category information.
    5. For 'description' or 'notice_details' - extract as much structured information as possible.
    6. Generate standard URL format for ADB notices.
    7. Always look for 'estimated_value', 'contract_value', or similar fields for financial information.
    8. Pay attention to 'project_number' or 'project_id' fields for reference information.
    `;
  }
}

module.exports = AdbAdapter; 
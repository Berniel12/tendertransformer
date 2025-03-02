/**
 * TedEuAdapter.js
 * Adapter for processing Tenders Electronic Daily (EU) tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class TedEuAdapter extends BaseSourceAdapter {
  constructor() {
    super('ted_eu');
  }
  
  /**
   * Extract source ID from TED EU tender
   */
  getSourceId(tender) {
    return tender.document_id?.toString() || tender.notice_id?.toString() || tender.id?.toString();
  }
  
  /**
   * Map TED EU fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.title || tender.notice_title,
      description: tender.description || tender.short_description,
      publication_date: tender.publication_date || tender.date_published ? new Date(tender.publication_date || tender.date_published).toISOString().split('T')[0] : null,
      deadline_date: tender.deadline_date || tender.submission_deadline ? new Date(tender.deadline_date || tender.submission_deadline).toISOString().split('T')[0] : null,
      notice_id: tender.document_id?.toString() || tender.notice_id?.toString(),
      reference_number: tender.reference_number || tender.document_number,
      organization_id: tender.contracting_authority || tender.contracting_body || tender.organization,
      country: tender.country || tender.country_code,
      status: this.mapStatus(tender.status),
      tender_type: this.mapNoticeType(tender.notice_type) || "Tender",
      sector: tender.sector || tender.activity_type || tender.cpv_code
    };
  }
  
  /**
   * Generate standard URL for TED EU tenders
   */
  generateUrl(tender) {
    if (tender.url) {
      return tender.url;
    } else if (tender.document_id) {
      return `https://ted.europa.eu/udl?uri=TED:NOTICE:${tender.document_id}:TEXT:EN:HTML`;
    }
    return null;
  }
  
  /**
   * Map TED EU specific status to standard status
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
      "completed": "Closed"
    };
    
    return statusMap[status.toLowerCase()] || status;
  }
  
  /**
   * Map TED EU notice types to standard tender types
   */
  mapNoticeType(noticeType) {
    if (!noticeType) return null;
    
    const typeMap = {
      "contract notice": "Tender",
      "prior information notice": "Prior Information Notice",
      "contract award notice": "Contract Award",
      "contract award": "Contract Award",
      "design contest notice": "Design Contest",
      "design contest result": "Results of Contest",
      "voluntary ex ante transparency notice": "Voluntary Ex Ante Notice",
      "modification notice": "Modification Notice",
      "corrigendum": "Corrigendum",
      "buyer profile": "Buyer Profile",
      "qualification system": "Qualification System"
    };
    
    return typeMap[noticeType.toLowerCase()] || noticeType;
  }
  
  /**
   * Get TED EU specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR TED (TENDERS ELECTRONIC DAILY - EU) DATA:
    1. For 'contracting_authority', 'contracting_body', or 'organization' field - use as the organization name.
    2. Extract country information from 'country' or map from 'country_code' fields.
    3. For 'notice_type' - map to standard tender types using these rules:
       - "contract notice" → "Tender"
       - "prior information notice" → "Prior Information Notice"
       - "contract award notice" → "Contract Award"
       - "design contest notice" → "Design Contest"
       - "design contest result" → "Results of Contest"
    4. For 'sector', 'activity_type', or 'cpv_code' - use as category information, especially CPV codes.
    5. Handle multilingual content - prioritize English but preserve important information in other EU languages if needed.
    6. Extract financial information from 'value', 'estimated_value', or similar fields, being careful about currency conversion.
    7. Pay special attention to NUTS codes for location information beyond country level.
    8. Generate standard URL format using document_id: https://ted.europa.eu/udl?uri=TED:NOTICE:{document_id}:TEXT:EN:HTML
    `;
  }
}

module.exports = TedEuAdapter; 
/**
 * AfdAdapter.js
 * Adapter for processing Agence Française de Développement tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class AfdAdapter extends BaseSourceAdapter {
  constructor() {
    super('afd_tenders');
  }
  
  /**
   * Extract source ID from AFD tender
   */
  getSourceId(tender) {
    return tender.tender_id?.toString() || tender.id?.toString();
  }
  
  /**
   * Map AFD fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.title || tender.tender_title,
      description: tender.description || tender.tender_description,
      publication_date: tender.publication_date ? new Date(tender.publication_date).toISOString().split('T')[0] : null,
      deadline_date: tender.deadline_date ? new Date(tender.deadline_date).toISOString().split('T')[0] : null,
      notice_id: tender.tender_id?.toString() || tender.notice_number?.toString(),
      reference_number: tender.reference_number || tender.tender_reference,
      organization_id: tender.organization || tender.contracting_authority,
      country: tender.country || tender.location,
      status: this.mapStatus(tender.status),
      tender_type: tender.tender_type || "Tender",
      sector: tender.sector || tender.category
    };
  }
  
  /**
   * Generate standard URL for AFD tenders
   */
  generateUrl(tender) {
    if (tender.url) {
      return tender.url;
    }
    return null;
  }
  
  /**
   * Map AFD specific status to standard status
   */
  mapStatus(status) {
    if (!status) return null;
    
    const statusMap = {
      "active": "Open",
      "en cours": "Open", // French for "in progress"
      "clôturé": "Closed", // French for "closed"
      "terminé": "Closed", // French for "finished"
      "attribué": "Awarded", // French for "awarded"
      "annulé": "Canceled", // French for "canceled"
      "closed": "Closed",
      "awarded": "Awarded",
      "canceled": "Canceled"
    };
    
    return statusMap[status.toLowerCase()] || status;
  }
  
  /**
   * Get AFD specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR AGENCE FRANÇAISE DE DÉVELOPPEMENT DATA:
    1. For 'contracting_authority' or 'organization' field - use as the organization name.
    2. Extract country information from 'country' or 'location' fields.
    3. For French language entries - translate important fields to English where possible.
    4. For 'sector' or 'category' - use as category information.
    5. Map status values from French to English:
       - "en cours" → "Open"
       - "clôturé" → "Closed"
       - "terminé" → "Closed"
       - "attribué" → "Awarded"
       - "annulé" → "Canceled"
    6. Extract financial information from any available budget fields.
    7. Look for project information in related fields like 'project_name' or 'project_id'.
    `;
  }
}

module.exports = AfdAdapter; 
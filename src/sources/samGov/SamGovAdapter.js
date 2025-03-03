/**
 * SamGovAdapter.js
 * Adapter for processing SAM.gov tender data
 */

const BaseSourceAdapter = require('../BaseSourceAdapter');

class SamGovAdapter extends BaseSourceAdapter {
  constructor() {
    super('sam_gov');
  }
  
  /**
   * Extract source ID from SAM.gov tender
   */
  getSourceId(tender) {
    return tender.opportunity_id?.toString();
  }
  
  /**
   * Map SAM.gov fields to standardized fields
   */
  mapFields(tender) {
    return {
      title: tender.opportunity_title,
      description: tender.description,
      publication_date: tender.publish_date ? new Date(tender.publish_date).toISOString().split('T')[0] : null,
      deadline_date: tender.response_date ? new Date(tender.response_date).toISOString().split('T')[0] : null,
      notice_id: tender.opportunity_id?.toString(),
      reference_number: tender.solicitation_number,
      organization_id: tender.organization_id,
      country: "UNITED STATES", // Default for SAM.gov
      status: this.mapStatus(tender.opportunity_status),
      tender_type: this.mapOpportunityType(tender.opportunity_type),
      sector: this.inferSectorFromNAICS(tender.naics_code),
      estimated_value: this.extractNumericValue(tender.estimated_value || tender.potential_award_amount || tender.award_amount),
      contract_value: this.extractNumericValue(tender.contract_value || tender.award_amount || tender.potential_award_amount)
    };
  }
  
  /**
   * Generate standard URL for SAM.gov tenders
   */
  generateUrl(tender) {
    if (tender.opportunity_id) {
      return `https://sam.gov/opp/${tender.opportunity_id}/view`;
    }
    return null;
  }
  
  /**
   * Map SAM.gov specific opportunity status to standard status
   */
  mapStatus(status) {
    if (!status) return null;
    
    const statusMap = {
      "active": "Open",
      "inactive": "Closed",
      "archived": "Closed",
      "awarded": "Awarded",
      "canceled": "Canceled"
    };
    
    return statusMap[status.toLowerCase()] || status;
  }
  
  /**
   * Map SAM.gov opportunity types to standard tender types
   */
  mapOpportunityType(type) {
    if (!type) return null;
    
    const typeMap = {
      "solicitation": "Tender",
      "presolicitation": "Prior Information Notice",
      "award": "Contract Award",
      "intent_to_award": "Intent to Award",
      "sources_sought": "Request for Information",
      "special_notice": "Special Notice",
      "sale_of_surplus": "Sale",
      "combined_synopsis_solicitation": "Request for Proposal"
    };
    
    return typeMap[type.toLowerCase()] || type;
  }
  
  /**
   * Infer sector from NAICS code
   */
  inferSectorFromNAICS(code) {
    if (!code) return null;
    
    const naicsPrefix = code.substring(0, 3);
    const naicsFirstTwo = code.substring(0, 2);
    
    if (naicsPrefix === '541') return "Information Technology";
    if (['236', '237', '238'].includes(naicsPrefix)) return "Construction";
    if (['31', '32', '33'].includes(naicsFirstTwo)) return "Manufacturing";
    if (naicsFirstTwo === '54') return "Professional Services";
    if (naicsFirstTwo === '62') return "Healthcare";
    if (naicsPrefix === '336') return "Defense and Aerospace";
    if (naicsFirstTwo === '48') return "Transportation";
    if (naicsFirstTwo === '22') return "Utilities";
    if (naicsFirstTwo === '11') return "Agriculture";
    
    return null;
  }
  
  /**
   * Get SAM.gov specific prompt additions for the LLM
   */
  getSourceSpecificPrompt() {
    return `
    SPECIFIC INSTRUCTIONS FOR SAM.GOV DATA:
    1. For 'place_of_performance' JSON field - extract country, state, city, and zip code information.
    2. For 'contacts' JSON array - identify primary contact if possible and extract all details.
    3. Map opportunity types to standard tender types:
       - "solicitation" → "Tender"
       - "presolicitation" → "Prior Information Notice" 
       - "award" → "Contract Award"
       - "intent_to_award" → "Intent to Award"
       - "sources_sought" → "Request for Information"
       - "special_notice" → "Special Notice"
       - "combined_synopsis_solicitation" → "Request for Proposal"
    4. Map opportunity statuses to standard statuses:
       - "active" → "Open"
       - "inactive" → "Closed"
       - "archived" → "Closed"
       - "awarded" → "Awarded"
       - "canceled" → "Canceled"
    5. For NAICS codes, include the industry sector in the 'sector' field:
       - Codes starting with "541" → "Information Technology"
       - Codes starting with "236", "237", "238" → "Construction"
       - Codes starting with "31", "32", "33" → "Manufacturing"
       - Codes starting with "54" → "Professional Services"
       - Codes starting with "62" → "Healthcare"
       - Codes starting with "336" → "Defense and Aerospace"
       - Codes starting with "48" → "Transportation"
       - Codes starting with "22" → "Utilities"
       - Codes starting with "11" → "Agriculture"
    6. Handle set-aside types like "Small Business", "8(a) Sole Source", etc. and include in description.
    7. Generate standard URL format: https://sam.gov/opp/{opportunity_id}/view
    8. Always look for 'potential_award_amount' or similar fields for financial information.
    `;
  }
}

module.exports = SamGovAdapter;
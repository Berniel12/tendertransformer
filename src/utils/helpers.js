/**
 * BaseSourceAdapter.js
 * Abstract base class for all tender source adapters
 */

class BaseSourceAdapter {
  /**
   * Constructor for the base adapter
   * @param {string} name - The name of the source table
   */
  constructor(name) {
    this.sourceName = name;
  }
  
  /**
   * Get a unique source identifier for a tender
   * @param {Object} tender - The tender data from the source table
   * @returns {string} A unique identifier
   */
  getSourceId(tender) { 
    throw new Error('getSourceId must be implemented by subclass'); 
  }
  
  /**
   * Map source-specific fields to standard fields
   * @param {Object} tender - The tender data from the source table
   * @returns {Object} Mapped fields
   */
  mapFields(tender) { 
    throw new Error('mapFields must be implemented by subclass'); 
  }
  
  /**
   * Generate a URL for the tender
   * @param {Object} tender - The tender data from the source table
   * @returns {string|null} The URL to the tender
   */
  generateUrl(tender) { 
    throw new Error('generateUrl must be implemented by subclass'); 
  }
  
  /**
   * Get LLM prompt additions specific to this source
   * @returns {string} Source-specific prompt additions
   */
  getSourceSpecificPrompt() {
    return '';
  }
  
  /**
   * Process a tender using this adapter
   * @param {Object} tender - The tender data from the source table
   * @param {Function} normalizeTenderWithLLM - Function to normalize tender with LLM
   * @returns {Promise<Object>} Normalized tender data
   */
  async processTender(tender, normalizeTenderWithLLM) {
    const sourceId = this.getSourceId(tender);
    const mappedFields = this.mapFields(tender);
    
    // Process with LLM
    const normalizedData = await normalizeTenderWithLLM(
      tender, 
      this.sourceName,
      this.getSourceSpecificPrompt()
    );
    
    // Apply source-specific field defaults if LLM didn't provide them
    Object.entries(mappedFields).forEach(([key, value]) => {
      if (!normalizedData[key] && value !== null && value !== undefined) {
        normalizedData[key] = value;
      }
    });
    
    // Apply source-specific URL generation if needed
    if (!normalizedData.url) {
      normalizedData.url = this.generateUrl(tender);
    }
    
    // Ensure source metadata is set
    normalizedData.source_table = this.sourceName;
    normalizedData.source_id = sourceId;
    normalizedData.original_data = tender;
    
    return normalizedData;
  }
}

module.exports = BaseSourceAdapter;
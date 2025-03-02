/**
 * sourceRegistry.js
 * Registry for all tender source adapters
 */

// Import adapters
const SamGovAdapter = require('../sources/samGov/SamGovAdapter');
const WbAdapter = require('../sources/wb/WbAdapter');
const AdbAdapter = require('../sources/adb/AdbAdapter');
const AfdAdapter = require('../sources/afd_tenders/AfdAdapter');
const AfdbAdapter = require('../sources/afdb/AfdbAdapter');
const AiibAdapter = require('../sources/aiib/AiibAdapter');
const IadbAdapter = require('../sources/iadb/IadbAdapter');
const TedEuAdapter = require('../sources/ted_eu/TedEuAdapter');
const UngmAdapter = require('../sources/ungm/UngmAdapter');

/**
 * Registry for all tender source adapters
 */
class SourceRegistry {
  constructor() {
    this.adapters = {};
    this.registerDefaults();
  }
  
  /**
   * Register a source adapter
   * @param {string} name - The source table name
   * @param {BaseSourceAdapter} adapter - The adapter instance
   */
  register(name, adapter) {
    console.log(`Registering adapter for source: ${name}`);
    this.adapters[name] = adapter;
  }
  
  /**
   * Get an adapter for a specific source
   * @param {string} tableName - The source table name
   * @returns {BaseSourceAdapter|null} The adapter or null if not found
   */
  getAdapter(tableName) {
    if (!this.adapters[tableName]) {
      console.warn(`No adapter registered for source: ${tableName}`);
      return null;
    }
    return this.adapters[tableName];
  }
  
  /**
   * Register default adapters for known sources
   */
  registerDefaults() {
    // Register all adapters
    this.register('sam_gov', new SamGovAdapter());
    this.register('wb', new WbAdapter());
    this.register('adb', new AdbAdapter());
    this.register('afd_tenders', new AfdAdapter());
    this.register('afdb', new AfdbAdapter());
    this.register('aiib', new AiibAdapter());
    this.register('iadb', new IadbAdapter());
    this.register('ted_eu', new TedEuAdapter());
    this.register('ungm', new UngmAdapter());
  }
  
  /**
   * Check if an adapter exists for a source
   * @param {string} tableName - The source table name
   * @returns {boolean} True if adapter exists
   */
  hasAdapter(tableName) {
    return !!this.adapters[tableName];
  }
  
  /**
   * Get a list of all registered sources
   * @returns {string[]} Array of source names
   */
  getRegisteredSources() {
    return Object.keys(this.adapters);
  }
}

// Create and export a singleton instance
const sourceRegistry = new SourceRegistry();
module.exports = sourceRegistry;
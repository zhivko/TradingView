const puppeteer = require('puppeteer');

async function testAutoscalingIssue() {
  console.log('🚀 Starting Puppeteer test for autoscaling issue...');

  const browser = await puppeteer.launch({
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--disable-gpu'
    ]
  });

  try {
    const page = await browser.newPage();

    // Set viewport for consistent testing
    await page.setViewport({ width: 1920, height: 1080 });

    // Collect console messages
    const consoleMessages = [];
    page.on('console', msg => {
      const text = msg.text();
      consoleMessages.push(text);
      console.log('CONSOLE:', text);
    });

    console.log('📄 Navigating to the crypto chart application...');
    await page.goto('http://localhost:5000', {
      waitUntil: 'networkidle0',
      timeout: 30000
    });

    console.log('👤 Testing guest mode login...');

    // Wait for the email modal to appear
    await page.waitForSelector('#email-modal', { visible: true, timeout: 10000 });

    // Click the "Continue as Guest" button
    await page.click('#modal-guest-btn');

    // Wait for the modal to disappear (indicating successful login)
    await page.waitForFunction(
      () => document.getElementById('email-modal').style.display === 'none',
      { timeout: 10000 }
    );

    console.log('✅ Guest login successful!');

    // Wait for the chart to load
    await page.waitForSelector('#chart', { timeout: 15000 });

    console.log('📊 Chart loaded, waiting for initial data...');

    // Wait for data to load (look for data points > 0)
    await page.waitForFunction(
      () => {
        const dataPoints = document.getElementById('data-points');
        return dataPoints && parseInt(dataPoints.textContent) > 0;
      },
      { timeout: 30000 }
    );

    const dataPoints = await page.$eval('#data-points', el => el.textContent);
    console.log(`📈 Initial data loaded: ${dataPoints} data points`);

    // Wait a bit more to see if there are any unwanted fetches
    console.log('⏳ Waiting 5 seconds to monitor for unwanted autoscaling...');
    await page.waitForTimeout(5000);

    // Analyze console messages for autoscaling issues
    console.log('\n🔍 Analyzing console messages for autoscaling issues...');

    const autoscaleMessages = consoleMessages.filter(msg =>
      msg.includes('User zoom/pan operation detected') ||
      msg.includes('Range changed (including autoscale)') ||
      msg.includes('Fetching data for range') ||
      msg.includes('Debounce timeout, fetching new data') ||
      msg.includes('plotChart called') ||
      msg.includes('Chart plotted, adding relayout listener')
    );

    console.log(`Found ${autoscaleMessages.length} relevant messages:`);
    autoscaleMessages.forEach((msg, i) => {
      console.log(`${i + 1}. ${msg}`);
    });

    // Check for the problematic sequence
    const hasUnwantedFetch = consoleMessages.some(msg =>
      msg.includes('User zoom/pan operation detected, fetching new data for zoomed range') &&
      consoleMessages.some(laterMsg =>
        laterMsg.includes('Range changed (including autoscale), debouncing...') &&
        consoleMessages.indexOf(laterMsg) > consoleMessages.indexOf(msg)
      )
    );

    if (hasUnwantedFetch) {
      console.log('❌ ISSUE DETECTED: Unwanted autoscaling/fetching after initial data load');
      throw new Error('Autoscaling issue reproduced');
    } else {
      console.log('✅ No unwanted autoscaling detected');
    }

    // Take a screenshot for verification
    await page.screenshot({
      path: 'autoscale_test_result.png',
      fullPage: true
    });

    console.log('📸 Screenshot saved as autoscale_test_result.png');

  } catch (error) {
    console.error('❌ Test failed:', error);
    throw error;
  } finally {
    await browser.close();
  }
}

// Run the test
if (require.main === module) {
  testAutoscalingIssue()
    .then(() => {
      console.log('✅ Test completed successfully - no autoscaling issue detected');
      process.exit(0);
    })
    .catch((error) => {
      console.error('💥 Test failed - autoscaling issue reproduced:', error);
      process.exit(1);
    });
}

module.exports = { testAutoscalingIssue };
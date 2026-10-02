import { expect, test } from "@playwright/test";
import type { Page } from "@playwright/test";

test("member and system admin receive different control surfaces", async ({ page }) => {
  await page.goto("/");
  await signIn(page, "user@example.com", "user12345!");

  await expect(page.getByRole("heading", { name: /Alice/ })).toBeVisible();
  await expect(page.getByRole("link", { name: "Usage & cost" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Access keys" })).toHaveCount(0);
  await expect(page.getByText("Vendor cost")).toHaveCount(0);
  await page.getByRole("link", { name: "Availability" }).click();
  await expect(page.getByRole("heading", { name: "Models" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Gateway instances" })).toHaveCount(0);

  await page.getByRole("button", { name: "Sign out" }).click();
  await signIn(page, "admin@example.com", "admin12345!");

  await expect(page.getByRole("link", { name: "Users & roles" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Configuration" })).toBeVisible();
  await expect(page.getByText("Vendor cost")).toBeVisible();
  await page.getByRole("link", { name: "Availability" }).click();
  await expect(page.getByRole("heading", { name: "Gateway instances" })).toBeVisible();
  await expect(page.getByText("gw-a", { exact: true })).toBeVisible();
  await expect(page.getByText("gw-b", { exact: true })).toBeVisible();

  await page.getByRole("link", { name: "Configuration" }).click();
  await expect(page.getByText(/Current version \d+/)).toBeVisible();
  await page.getByRole("button", { name: "Validate" }).click();
  await expect(page.getByText("Configuration is valid and ready to publish.")).toBeVisible();

  page.on("dialog", (dialog) => void dialog.accept());
  await page.getByRole("button", { name: "Publish configuration" }).click();
  await expect(page.getByText(/Published as version \d+/)).toBeVisible();

  await page.getByRole("button", { name: "Restore" }).first().click();
  await expect(page.getByText(/restored as version \d+/)).toBeVisible();
});

test("a generated key is shown once and listed only by its id", async ({ page }) => {
  await page.goto("/");
  await signIn(page, "admin@example.com", "admin12345!");
  await page.getByRole("link", { name: "Access keys" }).click();
  await page.getByRole("button", { name: "New key" }).click();
  await page.getByLabel("Tenant", { exact: true }).fill("acme");
  await page.getByRole("button", { name: "Create key" }).click();

  const issued = page.getByText(/^gw-[0-9a-f]{64}$/);
  await expect(issued).toBeVisible();
  const key = (await issued.textContent()) ?? "";
  await expect(page.getByRole("cell", { name: /^sha256:[0-9a-f]{32}/ }).first()).toBeVisible();
  await page.getByRole("button", { name: "Dismiss" }).click();
  await expect(page.getByText(key)).toHaveCount(0);
});

test("a tenant admin edits a user's budget and sees only its tenant", async ({ page }) => {
  await page.goto("/");
  await signIn(page, "manager@example.com", "manager123!");

  await page.getByRole("link", { name: "Users & roles" }).click();
  await expect(page.getByText("alice", { exact: true })).toBeVisible();
  await expect(page.getByText("admin@example.com")).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Add user" })).toHaveCount(0);

  const aliceBudget = page.getByRole("row", { name: /Alice Chen/ }).getByRole("button", { name: "Budget" });
  await aliceBudget.click();
  await expect(page.getByRole("spinbutton", { name: "Daily cost (USD) limit" })).toHaveValue("5");
  await page.getByRole("combobox", { name: "Monthly cost (USD) mode" }).selectOption("limit");
  await page.getByRole("spinbutton", { name: "Monthly cost (USD) limit" }).fill("40");
  await page.getByRole("button", { name: "Save budget" }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);

  await aliceBudget.click();
  await expect(page.getByRole("spinbutton", { name: "Monthly cost (USD) limit" })).toHaveValue("40");
  await page.getByRole("button", { name: "Reset to tenant defaults" }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await aliceBudget.click();
  await expect(page.getByRole("combobox", { name: "Daily cost (USD) mode" })).toHaveValue("inherit");
});

test("failed login shows an error and grants nothing", async ({ page }) => {
  await page.goto("/");
  await page.getByLabel("Email").fill("admin@example.com");
  await page.getByLabel("Password").fill("wrong-password!");
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByText("invalid email or password")).toBeVisible();
  await expect(page.getByRole("navigation", { name: "Main navigation" })).toHaveCount(0);
});

async function signIn(page: Page, email: string, password: string) {
  await page.getByLabel("Email").fill(email);
  await page.getByLabel("Password").fill(password);
  await page.getByRole("button", { name: "Sign in" }).click();
  await expect(page.getByRole("navigation", { name: "Main navigation" })).toBeVisible();
}

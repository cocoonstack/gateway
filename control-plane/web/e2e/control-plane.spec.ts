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
